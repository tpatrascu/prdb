use super::codec::*;
use super::*;
use crate::client_protocol::{CLIENT_MAX_CONTROL_BYTES, JsonStringObjectParser};
use std::collections::HashMap;
use zeroize::{Zeroize, Zeroizing};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ResponseValue {
    String(String),
    Number(u64),
    Boolean(bool),
    Object(HashMap<String, ResponseValue>),
    Array(Vec<ResponseValue>),
    Null,
}

pub(super) struct ZeroizingResponseFields(HashMap<String, ResponseValue>);

impl Zeroize for ZeroizingResponseFields {
    fn zeroize(&mut self) {
        for (mut key, mut value) in self.0.drain() {
            key.zeroize();
            value.zeroize();
        }
    }
}

impl std::ops::Deref for ZeroizingResponseFields {
    type Target = HashMap<String, ResponseValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ZeroizingResponseFields {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Zeroize for ResponseValue {
    fn zeroize(&mut self) {
        match self {
            Self::String(value) => value.zeroize(),
            Self::Object(fields) => {
                let mut fields = std::mem::take(fields);
                for (mut key, mut value) in fields.drain() {
                    key.zeroize();
                    value.zeroize();
                }
            }
            Self::Array(values) => {
                for value in values.iter_mut() {
                    value.zeroize();
                }
                values.clear();
            }
            Self::Number(_) | Self::Boolean(_) | Self::Null => {}
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum AdminExpectedResponseField<'a> {
    Id(&'a str, [u8; 16]),
    String(&'a str, &'a str),
    Boolean(&'a str, bool),
    PositiveInteger(&'a str),
}

impl AdminExpectedResponseField<'_> {
    pub(super) fn matches(self, fields: &HashMap<String, ResponseValue>) -> bool {
        match self {
            Self::Id(key, expected) => response_id(fields, key).ok() == Some(expected),
            Self::String(key, expected) => {
                fields.get(key).and_then(ResponseValue::as_string) == Some(expected)
            }
            Self::Boolean(key, expected) => {
                fields.get(key) == Some(&ResponseValue::Boolean(expected))
            }
            Self::PositiveInteger(key) => fields
                .get(key)
                .and_then(ResponseValue::as_string)
                .and_then(|text| parse_canonical_positive_u64(text.to_owned()))
                .is_some(),
        }
    }
}

impl ResponseValue {
    pub(crate) fn as_string(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub(super) fn as_number(&self) -> Option<u64> {
        match self {
            Self::Number(value) => Some(*value),
            _ => None,
        }
    }

    pub(super) fn as_decimal_string(&self) -> Option<&str> {
        match self {
            Self::String(value)
                if !value.is_empty()
                    && (value.len() == 1 || !value.starts_with('0'))
                    && value.bytes().all(|byte| byte.is_ascii_digit()) =>
            {
                Some(value)
            }
            _ => None,
        }
    }

    pub(crate) fn as_object(&self) -> Option<&HashMap<String, ResponseValue>> {
        match self {
            Self::Object(value) => Some(value),
            _ => None,
        }
    }

    pub(crate) fn as_array(&self) -> Option<&[ResponseValue]> {
        match self {
            Self::Array(value) => Some(value),
            _ => None,
        }
    }
}

pub(super) fn parse_flat_response(
    payload: &[u8],
) -> Result<HashMap<String, ResponseValue>, ClientSubmitError> {
    parse_bounded_json_value(payload, CLIENT_MAX_CONTROL_BYTES)?
        .as_object()
        .cloned()
        .ok_or(ClientSubmitError::InvalidResponse)
}

pub(crate) fn parse_bounded_json_value(
    payload: &[u8],
    maximum_bytes: usize,
) -> Result<ResponseValue, ClientSubmitError> {
    if payload.len() > maximum_bytes {
        return Err(ClientSubmitError::InvalidResponse);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ClientSubmitError::InvalidResponse)?;
    let mut parser = JsonStringObjectParser {
        text: text.as_bytes(),
        offset: 0,
    };
    parser.skip_whitespace();
    let mut value = Zeroizing::new(parse_response_value(&mut parser, 0)?);
    parser.skip_whitespace();
    if parser.offset != parser.text.len() {
        return Err(ClientSubmitError::InvalidResponse);
    }
    Ok(std::mem::replace(&mut *value, ResponseValue::Null))
}

#[cfg(feature = "protocol-fuzzing")]
#[doc(hidden)]
pub fn fuzz_decode_bounded_response_json(payload: &[u8]) {
    let _ = parse_bounded_json_value(payload, CLIENT_MAX_CONTROL_BYTES);
}

pub(super) fn parse_response_object(
    parser: &mut JsonStringObjectParser<'_>,
    depth: usize,
) -> Result<HashMap<String, ResponseValue>, ClientSubmitError> {
    if depth > 16 {
        return Err(ClientSubmitError::InvalidResponse);
    }
    parser.skip_whitespace();
    parser
        .expect(b'{')
        .map_err(|_| ClientSubmitError::InvalidResponse)?;
    let mut fields = Zeroizing::new(ZeroizingResponseFields(HashMap::new()));
    parser.skip_whitespace();
    if parser.consume(b'}') {
        return Ok(std::mem::take(&mut fields.0));
    }
    loop {
        parser.skip_whitespace();
        let mut key = Zeroizing::new(
            parser
                .parse_string()
                .map_err(|_| ClientSubmitError::InvalidResponse)?,
        );
        parser.skip_whitespace();
        parser
            .expect(b':')
            .map_err(|_| ClientSubmitError::InvalidResponse)?;
        parser.skip_whitespace();
        let mut value = Zeroizing::new(parse_response_value(parser, depth + 1)?);
        if fields.contains_key(key.as_str()) {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let key = std::mem::take(&mut *key);
        let value = std::mem::replace(&mut *value, ResponseValue::Null);
        fields.insert(key, value);
        parser.skip_whitespace();
        if parser.consume(b'}') {
            return Ok(std::mem::take(&mut fields.0));
        }
        parser
            .expect(b',')
            .map_err(|_| ClientSubmitError::InvalidResponse)?;
    }
}

pub(super) fn parse_response_value(
    parser: &mut JsonStringObjectParser<'_>,
    depth: usize,
) -> Result<ResponseValue, ClientSubmitError> {
    if depth > 16 {
        return Err(ClientSubmitError::InvalidResponse);
    }
    parser.skip_whitespace();
    match parser.text.get(parser.offset) {
        Some(b'"') => Ok(ResponseValue::String(
            parser
                .parse_string()
                .map_err(|_| ClientSubmitError::InvalidResponse)?,
        )),
        Some(b'{') => Ok(ResponseValue::Object(parse_response_object(parser, depth)?)),
        Some(b'[') => {
            parser
                .expect(b'[')
                .map_err(|_| ClientSubmitError::InvalidResponse)?;
            let mut values = Zeroizing::new(Vec::new());
            parser.skip_whitespace();
            if parser.consume(b']') {
                return Ok(ResponseValue::Array(std::mem::take(&mut *values)));
            }
            loop {
                values.push(parse_response_value(parser, depth + 1)?);
                parser.skip_whitespace();
                if parser.consume(b']') {
                    return Ok(ResponseValue::Array(std::mem::take(&mut *values)));
                }
                parser
                    .expect(b',')
                    .map_err(|_| ClientSubmitError::InvalidResponse)?;
            }
        }
        Some(b't' | b'f') => Ok(ResponseValue::Boolean(
            parser
                .parse_boolean()
                .map_err(|_| ClientSubmitError::InvalidResponse)?,
        )),
        Some(byte) if byte.is_ascii_digit() => Ok(ResponseValue::Number(
            parser
                .parse_u64_number()
                .map_err(|_| ClientSubmitError::InvalidResponse)?,
        )),
        Some(b'n')
            if parser
                .text
                .get(parser.offset..parser.offset.saturating_add(4))
                == Some(b"null") =>
        {
            parser.offset += 4;
            Ok(ResponseValue::Null)
        }
        _ => Err(ClientSubmitError::InvalidResponse),
    }
}

pub(super) fn take_string(fields: &HashMap<String, ResponseValue>, key: &str) -> Option<String> {
    fields.get(key)?.as_string().map(ToOwned::to_owned)
}

pub(super) fn encode_base64(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = *chunk.get(1).unwrap_or(&0);
        let third = *chunk.get(2).unwrap_or(&0);
        output.push(TABLE[(first >> 2) as usize] as char);
        output.push(TABLE[(((first & 3) << 4) | (second >> 4)) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[(((second & 15) << 2) | (third >> 6)) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(third & 63) as usize] as char
        } else {
            '='
        });
    }
    output
}

pub(super) fn lower_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(super) fn decode_lower_hex_32(text: &str) -> Result<[u8; 32], ClientSubmitError> {
    decode_hex_array(text).ok_or(ClientSubmitError::InvalidResponse)
}

pub(super) fn decode_lower_hex(text: &str, expected_bytes: usize) -> bool {
    text.len() == expected_bytes * 2
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn decode_hex_array<const N: usize>(text: &str) -> Option<[u8; N]> {
    if !decode_lower_hex(text, N) {
        return None;
    }
    let mut bytes = [0u8; N];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Some(bytes)
}

pub(super) fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}
