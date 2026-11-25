use super::types::*;
use super::validate::*;
use crate::optimizer_binding::{BoundColumn, BoundPredicate};
use crate::tuple::ScalarType;

pub(super) fn put_name(output: &mut Vec<u8>, name: &str) -> Result<(), PlanError> {
    if !valid_identifier(name) {
        return Err(PlanError::InvalidRelationReference);
    }
    let byte_length = u8::try_from(name.len()).map_err(|_| PlanError::TooLarge)?;
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_name_header_record(
        byte_length,
    ));
    output.extend_from_slice(name.as_bytes());
    Ok(())
}

pub(super) fn put_column_list_header(
    output: &mut Vec<u8>,
    node_tag: u8,
    column_count: usize,
) -> Result<(), PlanError> {
    let column_count = u16::try_from(column_count).map_err(|_| PlanError::TooLarge)?;
    output.extend_from_slice(
        &crate::query_abi::encode_optimizer_plan_column_list_header_record(node_tag, column_count),
    );
    Ok(())
}

pub(super) fn put_column(output: &mut Vec<u8>, column: &BoundColumn) {
    let (type_code, parameters) = encode_scalar_type(column.scalar_type);
    output.extend_from_slice(&crate::query_abi::encode_optimizer_plan_column_record(
        column.column_id,
        column.ordinal,
        type_code,
        parameters,
        u8::from(column.nullable),
    ));
}

pub(super) fn encode_scalar_type(scalar_type: ScalarType) -> (u8, [u8; 4]) {
    match scalar_type {
        ScalarType::Boolean => (1, [0; 4]),
        ScalarType::SmallInt => (2, [0; 4]),
        ScalarType::Integer => (3, [0; 4]),
        ScalarType::BigInt => (4, [0; 4]),
        ScalarType::Decimal { precision, scale } => (5, [precision, scale, 0, 0]),
        ScalarType::Text => (6, [0; 4]),
        ScalarType::Varchar { max_chars } => (7, max_chars.to_be_bytes()),
        ScalarType::Bytea => (8, [0; 4]),
        ScalarType::Uuid => (9, [0; 4]),
        ScalarType::Date => (10, [0; 4]),
        ScalarType::TimestampTz => (11, [0; 4]),
    }
}

pub(super) fn put_predicate(
    output: &mut Vec<u8>,
    predicate: &BoundPredicate,
) -> Result<(), PlanError> {
    match predicate {
        BoundPredicate::Null => output.extend_from_slice(
            &crate::query_abi::encode_optimizer_plan_null_predicate_record(0x00),
        ),
        BoundPredicate::Boolean(value) => output.extend_from_slice(
            &crate::query_abi::encode_optimizer_plan_boolean_literal_predicate_record(
                0x01,
                u8::from(*value),
            ),
        ),
        BoundPredicate::Integer(value) => {
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_integer_predicate_record(0x02, *value),
            );
        }
        BoundPredicate::Text(value) => {
            let text_length = u16::try_from(value.len()).map_err(|_| PlanError::TooLarge)?;
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_text_predicate_header_record(
                    0x03,
                    text_length,
                ),
            );
            output.extend_from_slice(value.as_bytes());
        }
        BoundPredicate::Bytea(value) => {
            if value.len() > crate::optimizer_request::MAX_TEXT_LITERAL_BYTES {
                return Err(PlanError::InvalidPredicate);
            }
            let length = u16::try_from(value.len()).map_err(|_| PlanError::TooLarge)?;
            output.push(0x07);
            output.extend_from_slice(&length.to_be_bytes());
            output.extend_from_slice(value);
        }
        BoundPredicate::Date(value) => {
            output.push(0x04);
            output.extend_from_slice(&value.to_be_bytes());
        }
        BoundPredicate::Uuid(value) => {
            output.push(0x05);
            output.extend_from_slice(value);
        }
        BoundPredicate::TimestampTz(value) => {
            output.push(0x06);
            output.extend_from_slice(&value.to_be_bytes());
        }
        BoundPredicate::Decimal(value) => {
            output.push(0x08);
            output.extend_from_slice(&value.to_be_bytes());
        }
        BoundPredicate::Column(column) => {
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_column_predicate_record(
                    0x10,
                    column.column_id,
                ),
            );
        }
        BoundPredicate::Compare {
            operator,
            left,
            right,
        } => {
            let comparison_operator = match operator {
                crate::optimizer_request::Comparison::Equal => 0,
                crate::optimizer_request::Comparison::NotEqual => 1,
                crate::optimizer_request::Comparison::Less => 2,
                crate::optimizer_request::Comparison::LessOrEqual => 3,
                crate::optimizer_request::Comparison::Greater => 4,
                crate::optimizer_request::Comparison::GreaterOrEqual => 5,
            };
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_compare_predicate_header_record(
                    0x20,
                    comparison_operator,
                ),
            );
            put_predicate(output, left)?;
            put_predicate(output, right)?;
        }
        BoundPredicate::IsNull(child) | BoundPredicate::IsNotNull(child) => {
            let tag = if matches!(predicate, BoundPredicate::IsNull(_)) {
                0x40
            } else {
                0x41
            };
            if !matches!(child.as_ref(), BoundPredicate::Column(_)) {
                return Err(PlanError::InvalidPredicate);
            }
            output.push(tag);
            put_predicate(output, child)?;
        }
        BoundPredicate::And(children) | BoundPredicate::Or(children) => {
            let is_and = matches!(predicate, BoundPredicate::And(_));
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_boolean_predicate_header_record(
                    if is_and { 0x30 } else { 0x31 },
                    u8::try_from(children.len()).map_err(|_| PlanError::InvalidPredicate)?,
                ),
            );
            for child in children {
                put_predicate(output, child)?;
            }
        }
        BoundPredicate::Not(child) => {
            output.extend_from_slice(
                &crate::query_abi::encode_optimizer_plan_not_predicate_record(0x32),
            );
            put_predicate(output, child)?;
        }
    }
    if output.len() > MAX_PLAN_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(())
}

pub(crate) fn encode_bound_predicate_v1(predicate: &BoundPredicate) -> Result<Vec<u8>, PlanError> {
    let mut output = Vec::new();
    put_predicate(&mut output, predicate)?;
    if output.len() > crate::optimizer_request::MAX_PREDICATE_BYTES {
        return Err(PlanError::TooLarge);
    }
    Ok(output)
}

pub(crate) fn decode_bound_predicate_v1(
    bytes: &[u8],
    scan_columns: &[BoundColumn],
) -> Result<BoundPredicate, PlanError> {
    if bytes.is_empty() || bytes.len() > crate::optimizer_request::MAX_PREDICATE_BYTES {
        return Err(PlanError::InvalidPredicate);
    }
    let mut reader = PlanReader { bytes, offset: 0 };
    let predicate = reader.predicate(scan_columns)?;
    if !reader.is_empty() {
        return Err(PlanError::InvalidPredicate);
    }
    Ok(predicate)
}

pub(super) struct PlanReader<'a> {
    pub(super) bytes: &'a [u8],
    pub(super) offset: usize,
}

impl PlanReader<'_> {
    pub(super) fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }

    pub(super) fn take(&mut self, count: usize) -> Result<&[u8], PlanError> {
        let end = self.offset.checked_add(count).ok_or(PlanError::TooLarge)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(PlanError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    pub(super) fn u8(&mut self) -> Result<u8, PlanError> {
        Ok(self.take(1)?[0])
    }

    pub(super) fn u16(&mut self) -> Result<u16, PlanError> {
        Ok(u16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| PlanError::Truncated)?,
        ))
    }

    pub(super) fn u32(&mut self) -> Result<u32, PlanError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| PlanError::Truncated)?,
        ))
    }

    pub(super) fn name(&mut self) -> Result<String, PlanError> {
        let header =
            self.take(crate::query_abi::OPTIMIZER_PLAN_NAME_HEADER_RECORD_HEADER_BYTES as usize)?;
        let byte_length = crate::query_abi::decode_optimizer_plan_name_header_record(header)
            .ok_or(PlanError::Truncated)?;
        let length = usize::from(byte_length);
        let bytes = self.take(length)?;
        let name = std::str::from_utf8(bytes).map_err(|_| PlanError::InvalidEncoding)?;
        if !valid_identifier(name) {
            return Err(PlanError::InvalidRelationReference);
        }
        Ok(name.to_owned())
    }

    pub(super) fn node_count(&mut self) -> Result<u8, PlanError> {
        let header =
            self.take(crate::query_abi::OPTIMIZER_PLAN_NODE_COUNT_RECORD_HEADER_BYTES as usize)?;
        crate::query_abi::decode_optimizer_plan_node_count_record(header)
            .ok_or(PlanError::Truncated)
    }

    pub(super) fn filter_node_tag(&mut self, header_offset: usize) -> Result<(), PlanError> {
        let header_len = crate::query_abi::OPTIMIZER_PLAN_FILTER_NODE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let node_tag = crate::query_abi::decode_optimizer_plan_filter_node_record(header)
            .ok_or(PlanError::Truncated)?;
        if node_tag != 2 {
            return Err(PlanError::InvalidPipeline);
        }
        self.offset = end;
        Ok(())
    }

    pub(super) fn column_list_count(
        &mut self,
        header_offset: usize,
        expected_tag: u8,
        maximum: usize,
    ) -> Result<usize, PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_COLUMN_LIST_HEADER_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, count) =
            crate::query_abi::decode_optimizer_plan_column_list_header_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != expected_tag {
            return Err(PlanError::InvalidPipeline);
        }
        self.offset = end;
        let count = usize::from(count);
        if count > maximum {
            return Err(PlanError::TooLarge);
        }
        Ok(count)
    }

    pub(super) fn limit_node_value(&mut self, header_offset: usize) -> Result<u32, PlanError> {
        let header_len = crate::query_abi::OPTIMIZER_PLAN_LIMIT_NODE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, row_limit) =
            crate::query_abi::decode_optimizer_plan_limit_node_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 4 {
            return Err(PlanError::InvalidPipeline);
        }
        self.offset = end;
        Ok(row_limit)
    }

    pub(super) fn text_predicate_length(&mut self, header_offset: usize) -> Result<u16, PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_TEXT_PREDICATE_HEADER_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, text_length) =
            crate::query_abi::decode_optimizer_plan_text_predicate_header_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 0x03 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(text_length)
    }

    pub(super) fn integer_predicate_value(
        &mut self,
        header_offset: usize,
    ) -> Result<i64, PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_INTEGER_PREDICATE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, integer_value) =
            crate::query_abi::decode_optimizer_plan_integer_predicate_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 0x02 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(integer_value)
    }

    pub(super) fn column_predicate_id(
        &mut self,
        header_offset: usize,
    ) -> Result<[u8; 16], PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_COLUMN_PREDICATE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, column_id) =
            crate::query_abi::decode_optimizer_plan_column_predicate_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 0x10 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(column_id)
    }

    pub(super) fn compare_predicate_operator(
        &mut self,
        header_offset: usize,
    ) -> Result<u8, PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_COMPARE_PREDICATE_HEADER_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, comparison_operator) =
            crate::query_abi::decode_optimizer_plan_compare_predicate_header_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 0x20 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(comparison_operator)
    }

    pub(super) fn boolean_predicate_header(
        &mut self,
        header_offset: usize,
    ) -> Result<(u8, u8), PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_BOOLEAN_PREDICATE_HEADER_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, child_count) =
            crate::query_abi::decode_optimizer_plan_boolean_predicate_header_record(header)
                .ok_or(PlanError::Truncated)?;
        if !matches!(node_tag, 0x30 | 0x31) {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok((node_tag, child_count))
    }

    pub(super) fn not_predicate_tag(&mut self, header_offset: usize) -> Result<(), PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_NOT_PREDICATE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let node_tag = crate::query_abi::decode_optimizer_plan_not_predicate_record(header)
            .ok_or(PlanError::Truncated)?;
        if node_tag != 0x32 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(())
    }

    pub(super) fn null_predicate_tag(&mut self, header_offset: usize) -> Result<(), PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_NULL_PREDICATE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let node_tag = crate::query_abi::decode_optimizer_plan_null_predicate_record(header)
            .ok_or(PlanError::Truncated)?;
        if node_tag != 0x00 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        Ok(())
    }

    pub(super) fn boolean_literal_value(
        &mut self,
        header_offset: usize,
    ) -> Result<bool, PlanError> {
        let header_len =
            crate::query_abi::OPTIMIZER_PLAN_BOOLEAN_LITERAL_PREDICATE_RECORD_HEADER_BYTES as usize;
        let end = header_offset
            .checked_add(header_len)
            .ok_or(PlanError::TooLarge)?;
        let header = self
            .bytes
            .get(header_offset..end)
            .ok_or(PlanError::Truncated)?;
        let (node_tag, boolean_value) =
            crate::query_abi::decode_optimizer_plan_boolean_literal_predicate_record(header)
                .ok_or(PlanError::Truncated)?;
        if node_tag != 0x01 {
            return Err(PlanError::InvalidEncoding);
        }
        self.offset = end;
        match boolean_value {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(PlanError::InvalidEncoding),
        }
    }

    pub(super) fn column(&mut self) -> Result<BoundColumn, PlanError> {
        let bytes =
            self.take(crate::query_abi::OPTIMIZER_PLAN_COLUMN_RECORD_HEADER_BYTES as usize)?;
        let (column_id, ordinal, type_code, parameters, nullable) =
            crate::query_abi::decode_optimizer_plan_column_record(bytes)
                .ok_or(PlanError::Truncated)?;
        let nullable = match nullable {
            0 => false,
            1 => true,
            _ => return Err(PlanError::InvalidEncoding),
        };
        let scalar_type = decode_scalar_type(type_code, parameters)?;
        Ok(BoundColumn {
            column_id,
            ordinal,
            scalar_type,
            nullable,
        })
    }

    pub(super) fn predicate(
        &mut self,
        scan_columns: &[BoundColumn],
    ) -> Result<BoundPredicate, PlanError> {
        let mut nodes = 0;
        self.predicate_at_depth(scan_columns, 1, &mut nodes)
    }

    pub(super) fn predicate_at_depth(
        &mut self,
        scan_columns: &[BoundColumn],
        depth: usize,
        nodes: &mut usize,
    ) -> Result<BoundPredicate, PlanError> {
        if depth > crate::optimizer_request::MAX_PREDICATE_DEPTH {
            return Err(PlanError::InvalidPredicate);
        }
        *nodes += 1;
        if *nodes > crate::optimizer_request::MAX_PREDICATE_NODES {
            return Err(PlanError::InvalidPredicate);
        }
        let tag_offset = self.offset;
        let tag = self.u8()?;
        match tag {
            0x00 => {
                self.null_predicate_tag(tag_offset)?;
                Ok(BoundPredicate::Null)
            }
            0x01 => Ok(BoundPredicate::Boolean(
                self.boolean_literal_value(tag_offset)?,
            )),
            0x02 => Ok(BoundPredicate::Integer(
                self.integer_predicate_value(tag_offset)?,
            )),
            0x03 => {
                let length = usize::from(self.text_predicate_length(tag_offset)?);
                if length > crate::optimizer_request::MAX_TEXT_LITERAL_BYTES {
                    return Err(PlanError::TooLarge);
                }
                let text = std::str::from_utf8(self.take(length)?)
                    .map_err(|_| PlanError::InvalidEncoding)?
                    .to_owned();
                Ok(BoundPredicate::Text(text))
            }
            0x04 => {
                let days =
                    i32::from_be_bytes(self.take(4)?.try_into().map_err(|_| PlanError::Truncated)?);
                if !crate::tuple::date_day_count_is_valid(days) {
                    return Err(PlanError::InvalidPredicate);
                }
                Ok(BoundPredicate::Date(days))
            }
            0x05 => Ok(BoundPredicate::Uuid(
                self.take(16)?
                    .try_into()
                    .map_err(|_| PlanError::Truncated)?,
            )),
            0x06 => {
                let micros =
                    i64::from_be_bytes(self.take(8)?.try_into().map_err(|_| PlanError::Truncated)?);
                if !crate::tuple::timestamp_micros_is_valid(micros) {
                    return Err(PlanError::InvalidPredicate);
                }
                Ok(BoundPredicate::TimestampTz(micros))
            }
            0x07 => {
                let length = usize::from(self.u16()?);
                if length > crate::optimizer_request::MAX_TEXT_LITERAL_BYTES {
                    return Err(PlanError::TooLarge);
                }
                Ok(BoundPredicate::Bytea(self.take(length)?.to_vec()))
            }
            0x08 => Ok(BoundPredicate::Decimal(i128::from_be_bytes(
                self.take(16)?
                    .try_into()
                    .map_err(|_| PlanError::Truncated)?,
            ))),
            0x10 => {
                let column_id = self.column_predicate_id(tag_offset)?;
                let column = scan_columns
                    .iter()
                    .find(|column| column.column_id == column_id)
                    .ok_or(PlanError::InvalidColumnReference)?;
                Ok(BoundPredicate::Column(column.clone()))
            }
            0x20 => {
                let operator = match self.compare_predicate_operator(tag_offset)? {
                    0 => crate::optimizer_request::Comparison::Equal,
                    1 => crate::optimizer_request::Comparison::NotEqual,
                    2 => crate::optimizer_request::Comparison::Less,
                    3 => crate::optimizer_request::Comparison::LessOrEqual,
                    4 => crate::optimizer_request::Comparison::Greater,
                    5 => crate::optimizer_request::Comparison::GreaterOrEqual,
                    _ => return Err(PlanError::InvalidEncoding),
                };
                let left = Box::new(self.predicate_at_depth(scan_columns, depth + 1, nodes)?);
                let right = Box::new(self.predicate_at_depth(scan_columns, depth + 1, nodes)?);
                Ok(BoundPredicate::Compare {
                    operator,
                    left,
                    right,
                })
            }
            0x40 | 0x41 => {
                let child = Box::new(self.predicate_at_depth(scan_columns, depth + 1, nodes)?);
                if !matches!(child.as_ref(), BoundPredicate::Column(_)) {
                    return Err(PlanError::InvalidPredicate);
                }
                if tag == 0x40 {
                    Ok(BoundPredicate::IsNull(child))
                } else {
                    Ok(BoundPredicate::IsNotNull(child))
                }
            }
            0x30 | 0x31 => {
                let (tag, child_count) = self.boolean_predicate_header(tag_offset)?;
                let count = usize::from(child_count);
                if !(2..=16).contains(&count) {
                    return Err(PlanError::InvalidPredicate);
                }
                let mut children = Vec::with_capacity(count);
                for _ in 0..count {
                    children.push(self.predicate_at_depth(scan_columns, depth + 1, nodes)?);
                }
                if tag == 0x30 {
                    Ok(BoundPredicate::And(children))
                } else {
                    Ok(BoundPredicate::Or(children))
                }
            }
            0x32 => {
                self.not_predicate_tag(tag_offset)?;
                Ok(BoundPredicate::Not(Box::new(self.predicate_at_depth(
                    scan_columns,
                    depth + 1,
                    nodes,
                )?)))
            }
            _ => Err(PlanError::InvalidEncoding),
        }
    }
}

pub(super) fn decode_scalar_type(code: u8, parameters: [u8; 4]) -> Result<ScalarType, PlanError> {
    match code {
        1 if parameters == [0; 4] => Ok(ScalarType::Boolean),
        2 if parameters == [0; 4] => Ok(ScalarType::SmallInt),
        3 if parameters == [0; 4] => Ok(ScalarType::Integer),
        4 if parameters == [0; 4] => Ok(ScalarType::BigInt),
        5 if (1..=38).contains(&parameters[0])
            && parameters[1] <= parameters[0]
            && parameters[2..] == [0; 2] =>
        {
            Ok(ScalarType::Decimal {
                precision: parameters[0],
                scale: parameters[1],
            })
        }
        6 if parameters == [0; 4] => Ok(ScalarType::Text),
        7 if u32::from_be_bytes(parameters) != 0 => Ok(ScalarType::Varchar {
            max_chars: u32::from_be_bytes(parameters),
        }),
        8 if parameters == [0; 4] => Ok(ScalarType::Bytea),
        9 if parameters == [0; 4] => Ok(ScalarType::Uuid),
        10 if parameters == [0; 4] => Ok(ScalarType::Date),
        11 if parameters == [0; 4] => Ok(ScalarType::TimestampTz),
        _ => Err(PlanError::InvalidEncoding),
    }
}
