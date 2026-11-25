use super::ast::DdlSqlError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Token {
    Word(String),
    Number(String),
    String(String),
    Minus,
    Plus,
    Dot,
    Comma,
    LeftParen,
    RightParen,
    Semicolon,
    Equal,
    NotEqual,
    Less,
    LessEqual,
    Greater,
    GreaterEqual,
    Star,
    Percent,
    Slash,
}

pub(super) fn invalid<T>(_: T) -> DdlSqlError {
    DdlSqlError::InvalidStatement
}

pub(super) fn parse_decimal_coefficient(
    literal: &str,
    precision: u8,
    scale: u8,
) -> Result<i128, DdlSqlError> {
    let negative = literal.starts_with('-');
    let unsigned = literal.strip_prefix('-').unwrap_or(literal);
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > scale as usize
    {
        return Err(DdlSqlError::InvalidStatement);
    }
    let mut digits = whole.to_owned();
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n('0', scale as usize - fraction.len()));
    let significant = digits.trim_start_matches('0');
    if significant.len() > precision as usize {
        return Err(DdlSqlError::InvalidStatement);
    }
    let magnitude: i128 = if significant.is_empty() {
        0
    } else {
        significant.parse().map_err(invalid)?
    };
    Ok(if negative { -magnitude } else { magnitude })
}

pub(super) fn decode_hex(text: &str) -> Result<Vec<u8>, DdlSqlError> {
    if !text.len().is_multiple_of(2) {
        return Err(DdlSqlError::InvalidStatement);
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok((hex_digit(pair[0])? << 4) | hex_digit(pair[1])?))
        .collect()
}

pub(super) fn parse_uuid(text: &str) -> Result<[u8; 16], DdlSqlError> {
    if text.len() == 36 {
        for position in [8, 13, 18, 23] {
            if text.as_bytes().get(position) != Some(&b'-') {
                return Err(DdlSqlError::InvalidStatement);
            }
        }
    } else if text.len() != 32 {
        return Err(DdlSqlError::InvalidStatement);
    }
    let compact: String = text.chars().filter(|&ch| ch != '-').collect();
    let bytes = decode_hex(&compact)?;
    bytes.try_into().map_err(invalid)
}

pub(super) fn hex_digit(byte: u8) -> Result<u8, DdlSqlError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(DdlSqlError::InvalidStatement),
    }
}

pub(super) fn tokenize(sql: &str) -> Result<Vec<Token>, DdlSqlError> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut cursor = 0;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b' ' | b'\t' | b'\r' | b'\n' => cursor += 1,
            b'.' => {
                tokens.push(Token::Dot);
                cursor += 1;
            }
            b',' => {
                tokens.push(Token::Comma);
                cursor += 1;
            }
            b'(' => {
                tokens.push(Token::LeftParen);
                cursor += 1;
            }
            b')' => {
                tokens.push(Token::RightParen);
                cursor += 1;
            }
            b';' => {
                tokens.push(Token::Semicolon);
                cursor += 1;
            }
            b'=' => {
                tokens.push(Token::Equal);
                cursor += 1;
            }
            b'!' if bytes.get(cursor + 1) == Some(&b'=') => {
                tokens.push(Token::NotEqual);
                cursor += 2;
            }
            b'<' if bytes.get(cursor + 1) == Some(&b'=') => {
                tokens.push(Token::LessEqual);
                cursor += 2;
            }
            b'<' if bytes.get(cursor + 1) == Some(&b'>') => {
                tokens.push(Token::NotEqual);
                cursor += 2;
            }
            b'<' => {
                tokens.push(Token::Less);
                cursor += 1;
            }
            b'>' if bytes.get(cursor + 1) == Some(&b'=') => {
                tokens.push(Token::GreaterEqual);
                cursor += 2;
            }
            b'>' => {
                tokens.push(Token::Greater);
                cursor += 1;
            }
            b'-' => {
                tokens.push(Token::Minus);
                cursor += 1;
            }
            b'+' => {
                tokens.push(Token::Plus);
                cursor += 1;
            }
            b'*' => {
                tokens.push(Token::Star);
                cursor += 1;
            }
            b'%' => {
                tokens.push(Token::Percent);
                cursor += 1;
            }
            b'/' => {
                tokens.push(Token::Slash);
                cursor += 1;
            }
            b'\'' => {
                cursor += 1;
                let mut value = Vec::new();
                let mut closed = false;
                while cursor < bytes.len() {
                    if bytes[cursor] == b'\'' {
                        if bytes.get(cursor + 1) == Some(&b'\'') {
                            value.push(b'\'');
                            cursor += 2;
                        } else {
                            cursor += 1;
                            closed = true;
                            break;
                        }
                    } else {
                        value.push(bytes[cursor]);
                        cursor += 1;
                    }
                }
                if !closed {
                    return Err(DdlSqlError::InvalidStatement);
                }
                tokens.push(Token::String(String::from_utf8(value).map_err(invalid)?));
            }
            byte if byte.is_ascii_alphabetic() || byte == b'_' => {
                let start = cursor;
                cursor += 1;
                while cursor < bytes.len()
                    && (bytes[cursor].is_ascii_alphanumeric() || bytes[cursor] == b'_')
                {
                    cursor += 1;
                }
                let word = std::str::from_utf8(&bytes[start..cursor])
                    .map_err(|_| DdlSqlError::InvalidStatement)?;
                tokens.push(Token::Word(word.to_ascii_lowercase()));
            }
            byte if byte.is_ascii_digit() => {
                let start = cursor;
                cursor += 1;
                while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                    cursor += 1;
                }
                if bytes.get(cursor) == Some(&b'.')
                    && bytes.get(cursor + 1).is_some_and(u8::is_ascii_digit)
                {
                    cursor += 1;
                    while cursor < bytes.len() && bytes[cursor].is_ascii_digit() {
                        cursor += 1;
                    }
                }
                let number = std::str::from_utf8(&bytes[start..cursor])
                    .map_err(|_| DdlSqlError::InvalidStatement)?;
                tokens.push(Token::Number(number.to_owned()));
            }
            _ => return Err(DdlSqlError::InvalidStatement),
        }
    }
    Ok(tokens)
}
