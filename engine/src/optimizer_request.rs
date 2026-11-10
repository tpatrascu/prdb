//! Bounded version-1 structured query requests for the isolated optimizer.
//!
//! This decoder accepts only a logical single-relation SELECT request. It does
//! not resolve names or grant authority; those checks belong to the invocation
//! bound planner service and the executor/storage path.

const VERSION: u16 = 1;
const SELECT: u8 = 1;
pub const MAX_REQUEST_BYTES: usize = 64 * 1024;
pub const MAX_IDENTIFIER_BYTES: usize = 63;
pub const MAX_PROJECTIONS: usize = 128;
pub const MAX_PREDICATE_BYTES: usize = 16 * 1024;
pub const MAX_PREDICATE_NODES: usize = 256;
pub const MAX_PREDICATE_DEPTH: usize = 32;
pub const MAX_TEXT_LITERAL_BYTES: usize = 4096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SelectRequest {
    pub schema_name: String,
    pub relation_name: String,
    pub projections: Vec<String>,
    pub predicate: Option<Predicate>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Predicate {
    Null,
    Boolean(bool),
    Integer(i64),
    Text(String),
    Column(String),
    Compare {
        operator: Comparison,
        left: Box<Predicate>,
        right: Box<Predicate>,
    },
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Comparison {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestError {
    TooLarge,
    Truncated,
    UnsupportedVersion(u16),
    UnsupportedStatement(u8),
    InvalidValue,
    InvalidIdentifier,
    InvalidPredicate,
    PredicateTooLarge,
    TooManyProjections,
    TooManyPredicateNodes,
    PredicateTooDeep,
    TrailingBytes,
}

impl std::fmt::Display for RequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "optimizer request error: {self:?}")
    }
}

impl std::error::Error for RequestError {}

/// Decode the canonical v1 request. All lengths and collection bounds are
/// checked before allocating their corresponding owned values.
pub fn decode_select_request(bytes: &[u8]) -> Result<SelectRequest, RequestError> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(RequestError::TooLarge);
    }
    let mut cursor = Cursor::new(bytes);
    let version = cursor.u16()?;
    if version != VERSION {
        return Err(RequestError::UnsupportedVersion(version));
    }
    let statement = cursor.u8()?;
    if statement != SELECT {
        return Err(RequestError::UnsupportedStatement(statement));
    }
    if cursor.u8()? != 0 {
        return Err(RequestError::InvalidValue);
    }
    let schema_name = cursor.identifier()?;
    let relation_name = cursor.identifier()?;
    let projection_count = usize::from(cursor.u16()?);
    if projection_count == 0 || projection_count > MAX_PROJECTIONS {
        return Err(RequestError::TooManyProjections);
    }
    let mut projections = Vec::with_capacity(projection_count);
    for _ in 0..projection_count {
        projections.push(cursor.identifier()?);
    }

    let predicate_len = usize::try_from(cursor.u32()?).map_err(|_| RequestError::TooLarge)?;
    if predicate_len > MAX_PREDICATE_BYTES {
        return Err(RequestError::PredicateTooLarge);
    }
    let predicate_bytes = cursor.take(predicate_len)?;
    let predicate = if predicate_bytes.is_empty() {
        None
    } else {
        let mut parser = PredicateParser::new(predicate_bytes);
        let predicate = parser.node(1)?;
        if !parser.cursor.is_empty() {
            return Err(RequestError::TrailingBytes);
        }
        Some(predicate)
    };

    let limit = match cursor.u8()? {
        0 => None,
        1 => Some(cursor.u32()?),
        _ => return Err(RequestError::InvalidValue),
    };
    if !cursor.is_empty() {
        return Err(RequestError::TrailingBytes);
    }
    Ok(SelectRequest {
        schema_name,
        relation_name,
        projections,
        predicate,
        limit,
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], RequestError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(RequestError::TooLarge)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(RequestError::Truncated)?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, RequestError> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16, RequestError> {
        Ok(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| RequestError::Truncated)?,
        ))
    }

    fn u32(&mut self) -> Result<u32, RequestError> {
        Ok(u32::from_be_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| RequestError::Truncated)?,
        ))
    }

    fn identifier(&mut self) -> Result<String, RequestError> {
        let length = usize::from(self.u8()?);
        if length == 0 || length > MAX_IDENTIFIER_BYTES {
            return Err(RequestError::InvalidIdentifier);
        }
        let bytes = self.take(length)?;
        if !bytes[0].is_ascii_alphabetic() && bytes[0] != b'_' {
            return Err(RequestError::InvalidIdentifier);
        }
        if !bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        {
            return Err(RequestError::InvalidIdentifier);
        }
        String::from_utf8(bytes.to_vec()).map_err(|_| RequestError::InvalidIdentifier)
    }

    fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

struct PredicateParser<'a> {
    cursor: Cursor<'a>,
    nodes: usize,
}

impl<'a> PredicateParser<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self {
            cursor: Cursor::new(bytes),
            nodes: 0,
        }
    }

    fn node(&mut self, depth: usize) -> Result<Predicate, RequestError> {
        if depth > MAX_PREDICATE_DEPTH {
            return Err(RequestError::PredicateTooDeep);
        }
        self.nodes += 1;
        if self.nodes > MAX_PREDICATE_NODES {
            return Err(RequestError::TooManyPredicateNodes);
        }
        let tag = self.cursor.u8()?;
        match tag {
            0x00 => Ok(Predicate::Null),
            0x01 => match self.cursor.u8()? {
                0 => Ok(Predicate::Boolean(false)),
                1 => Ok(Predicate::Boolean(true)),
                _ => Err(RequestError::InvalidValue),
            },
            0x02 => Ok(Predicate::Integer(i64::from_be_bytes(
                self.cursor
                    .take(8)?
                    .try_into()
                    .map_err(|_| RequestError::Truncated)?,
            ))),
            0x03 => {
                let length = usize::from(self.cursor.u16()?);
                if length > MAX_TEXT_LITERAL_BYTES {
                    return Err(RequestError::InvalidValue);
                }
                let text = std::str::from_utf8(self.cursor.take(length)?)
                    .map_err(|_| RequestError::InvalidValue)?
                    .to_owned();
                Ok(Predicate::Text(text))
            }
            0x10 => Ok(Predicate::Column(self.cursor.identifier()?)),
            0x20 => {
                let operator = match self.cursor.u8()? {
                    0 => Comparison::Equal,
                    1 => Comparison::NotEqual,
                    2 => Comparison::Less,
                    3 => Comparison::LessOrEqual,
                    4 => Comparison::Greater,
                    5 => Comparison::GreaterOrEqual,
                    _ => return Err(RequestError::InvalidPredicate),
                };
                let left = Box::new(self.node(depth + 1)?);
                let right = Box::new(self.node(depth + 1)?);
                Ok(Predicate::Compare {
                    operator,
                    left,
                    right,
                })
            }
            0x30 | 0x31 => {
                let count = usize::from(self.cursor.u8()?);
                if !(2..=16).contains(&count) {
                    return Err(RequestError::InvalidPredicate);
                }
                let mut children = Vec::with_capacity(count);
                for _ in 0..count {
                    children.push(self.node(depth + 1)?);
                }
                if tag == 0x30 {
                    Ok(Predicate::And(children))
                } else {
                    Ok(Predicate::Or(children))
                }
            }
            0x32 => Ok(Predicate::Not(Box::new(self.node(depth + 1)?))),
            _ => Err(RequestError::InvalidPredicate),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u16_bytes(value: usize, output: &mut Vec<u8>) {
        output.extend_from_slice(&(value as u16).to_be_bytes());
    }

    fn u32_bytes(value: usize, output: &mut Vec<u8>) {
        output.extend_from_slice(&(value as u32).to_be_bytes());
    }

    fn name(value: &str, output: &mut Vec<u8>) {
        output.push(value.len() as u8);
        output.extend_from_slice(value.as_bytes());
    }

    fn select_request(predicate: &[u8]) -> Vec<u8> {
        let mut bytes = vec![0, 1, SELECT, 0];
        name("public", &mut bytes);
        name("items", &mut bytes);
        u16_bytes(2, &mut bytes);
        name("id", &mut bytes);
        name("label", &mut bytes);
        u32_bytes(predicate.len(), &mut bytes);
        bytes.extend_from_slice(predicate);
        bytes.extend_from_slice(&[1, 0, 0, 0, 10]);
        bytes
    }

    #[test]
    fn decodes_bounded_single_relation_request_and_predicate() {
        let mut expression = vec![0x30, 2, 0x20, 0, 0x10];
        name("id", &mut expression);
        expression.push(0x02);
        expression.extend_from_slice(&7i64.to_be_bytes());
        expression.extend_from_slice(&[0x20, 4, 0x10]);
        name("label", &mut expression);
        expression.push(0x03);
        u16_bytes(3, &mut expression);
        expression.extend_from_slice(b"new");

        let request = decode_select_request(&select_request(&expression)).unwrap();
        assert_eq!(request.schema_name, "public");
        assert_eq!(request.relation_name, "items");
        assert_eq!(request.projections, ["id", "label"]);
        assert_eq!(request.limit, Some(10));
        assert_eq!(
            request.predicate,
            Some(Predicate::And(vec![
                Predicate::Compare {
                    operator: Comparison::Equal,
                    left: Box::new(Predicate::Column("id".to_owned())),
                    right: Box::new(Predicate::Integer(7)),
                },
                Predicate::Compare {
                    operator: Comparison::Greater,
                    left: Box::new(Predicate::Column("label".to_owned())),
                    right: Box::new(Predicate::Text("new".to_owned())),
                },
            ]))
        );
    }

    #[test]
    fn rejects_malformed_outer_records_and_identifiers() {
        assert_eq!(decode_select_request(&[]), Err(RequestError::Truncated));
        let mut wrong_version = select_request(&[]);
        wrong_version[1] = 2;
        assert_eq!(
            decode_select_request(&wrong_version),
            Err(RequestError::UnsupportedVersion(2))
        );
        let mut unknown_statement = select_request(&[]);
        unknown_statement[2] = 9;
        assert_eq!(
            decode_select_request(&unknown_statement),
            Err(RequestError::UnsupportedStatement(9))
        );
        let mut invalid_name = select_request(&[]);
        invalid_name[4] = 1;
        invalid_name[5] = b'1';
        assert_eq!(
            decode_select_request(&invalid_name),
            Err(RequestError::InvalidIdentifier)
        );
        let mut trailing = select_request(&[]);
        trailing.push(0);
        assert_eq!(
            decode_select_request(&trailing),
            Err(RequestError::TrailingBytes)
        );
        assert_eq!(
            decode_select_request(&vec![0; MAX_REQUEST_BYTES + 1]),
            Err(RequestError::TooLarge)
        );
    }

    #[test]
    fn rejects_excessive_collections_and_unsupported_nodes() {
        let mut too_many_projection = select_request(&[]);
        let projection_count_offset = 4 + 1 + "public".len() + 1 + "items".len();
        too_many_projection[projection_count_offset..projection_count_offset + 2]
            .copy_from_slice(&((MAX_PROJECTIONS + 1) as u16).to_be_bytes());
        assert_eq!(
            decode_select_request(&too_many_projection),
            Err(RequestError::TooManyProjections)
        );
        assert_eq!(
            decode_select_request(&select_request(&[0x7f])),
            Err(RequestError::InvalidPredicate)
        );
        assert_eq!(
            decode_select_request(&select_request(&[0x01, 2])),
            Err(RequestError::InvalidValue)
        );
    }

    #[test]
    fn predicate_depth_and_node_budgets_fail_closed() {
        let mut deep = vec![0x32; MAX_PREDICATE_DEPTH + 1];
        deep.push(0x00);
        assert_eq!(
            decode_select_request(&select_request(&deep)),
            Err(RequestError::PredicateTooDeep)
        );

        let mut wide = vec![0x30, 16];
        for _ in 0..16 {
            wide.push(0x30);
            wide.push(16);
            wide.extend(std::iter::repeat_n(0x00, 16));
        }
        assert_eq!(
            decode_select_request(&select_request(&wide)),
            Err(RequestError::TooManyPredicateNodes)
        );
        assert_eq!(
            decode_select_request(&select_request(&vec![0x00; MAX_PREDICATE_BYTES + 1])),
            Err(RequestError::PredicateTooLarge)
        );
    }
}
