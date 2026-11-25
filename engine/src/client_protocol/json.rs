use super::types::*;
use super::*;
use crate::credentials::TOKEN_SECRET_BYTES;
use std::collections::HashMap;
use zeroize::{Zeroize, Zeroizing};

#[derive(Debug)]
pub(super) enum JsonScalar {
    String(String),
    Number(u64),
    Boolean(bool),
}

pub(super) fn parse_scalar_object(
    payload: &[u8],
) -> Result<HashMap<String, JsonScalar>, ClientControlError> {
    if payload.len() > CLIENT_MAX_CONTROL_BYTES {
        return Err(ClientControlError::InvalidJson);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ClientControlError::InvalidJson)?;
    let mut parser = JsonStringObjectParser {
        text: text.as_bytes(),
        offset: 0,
    };
    parser.skip_whitespace();
    parser.expect(b'{')?;
    let mut fields = HashMap::new();
    parser.skip_whitespace();
    if !parser.consume(b'}') {
        loop {
            parser.skip_whitespace();
            let key = parser.parse_string()?;
            parser.skip_whitespace();
            parser.expect(b':')?;
            parser.skip_whitespace();
            let value = if parser.text.get(parser.offset) == Some(&b'"') {
                JsonScalar::String(parser.parse_string()?)
            } else if parser
                .text
                .get(parser.offset)
                .is_some_and(u8::is_ascii_digit)
            {
                JsonScalar::Number(parser.parse_u64_number()?)
            } else {
                JsonScalar::Boolean(parser.parse_boolean()?)
            };
            if fields.insert(key, value).is_some() {
                return Err(ClientControlError::DuplicateKey);
            }
            parser.skip_whitespace();
            if parser.consume(b'}') {
                break;
            }
            parser.expect(b',')?;
        }
    }
    parser.skip_whitespace();
    if parser.offset != parser.text.len() {
        return Err(ClientControlError::InvalidJson);
    }
    Ok(fields)
}

pub(super) fn take_string(
    fields: &mut HashMap<String, JsonScalar>,
    key: &str,
) -> Result<String, ClientControlError> {
    match fields.remove(key).ok_or(ClientControlError::MissingKey)? {
        JsonScalar::String(value) => Ok(value),
        JsonScalar::Number(_) | JsonScalar::Boolean(_) => Err(ClientControlError::InvalidPrepare),
    }
}

pub(super) fn take_number(
    fields: &mut HashMap<String, JsonScalar>,
    key: &str,
) -> Result<u64, ClientControlError> {
    match fields.remove(key).ok_or(ClientControlError::MissingKey)? {
        JsonScalar::Number(value) => Ok(value),
        JsonScalar::String(_) | JsonScalar::Boolean(_) => Err(ClientControlError::InvalidPrepare),
    }
}

pub(super) fn take_bool(
    fields: &mut HashMap<String, JsonScalar>,
    key: &str,
) -> Result<bool, ClientControlError> {
    match fields.remove(key).ok_or(ClientControlError::MissingKey)? {
        JsonScalar::Boolean(value) => Ok(value),
        JsonScalar::String(_) | JsonScalar::Number(_) => Err(ClientControlError::InvalidPrepare),
    }
}

pub(super) fn take_decimal_string(
    fields: &mut HashMap<String, JsonScalar>,
    key: &str,
) -> Result<u64, ClientControlError> {
    let value = take_string(fields, key)?;
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    value
        .parse()
        .map_err(|_| ClientControlError::InvalidPrepare)
}

pub(super) fn decode_lower_hex_hash(text: &str) -> Result<[u8; 32], ClientControlError> {
    if text.len() != 64 {
        return Err(ClientControlError::InvalidPrepare);
    }
    let mut hash = [0; 32];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        hash[index] = (lower_hex_nibble(pair[0]).ok_or(ClientControlError::InvalidPrepare)? << 4)
            | lower_hex_nibble(pair[1]).ok_or(ClientControlError::InvalidPrepare)?;
    }
    Ok(hash)
}

pub(super) fn decode_base64_bytes(text: &str) -> Option<Vec<u8>> {
    let input = text.as_bytes();
    if input.is_empty() || !input.len().is_multiple_of(4) {
        return None;
    }
    let mut output = Vec::new();
    output.try_reserve_exact(input.len() / 4 * 3).ok()?;
    for (index, chunk) in input.chunks_exact(4).enumerate() {
        let last = index + 1 == input.len() / 4;
        let a = base64_value(chunk[0])?;
        let b = base64_value(chunk[1])?;
        output.push((a << 2) | (b >> 4));
        match (chunk[2], chunk[3]) {
            (b'=', b'=') if last && b & 0x0f == 0 => (),
            (c, b'=') if last => {
                let c = base64_value(c)?;
                if c & 0x03 != 0 {
                    return None;
                }
                output.push((b << 4) | (c >> 2));
            }
            (c, d) if c != b'=' && d != b'=' => {
                let c = base64_value(c)?;
                let d = base64_value(d)?;
                output.push((b << 4) | (c >> 2));
                output.push((c << 6) | d);
            }
            _ => return None,
        }
    }
    Some(output)
}

pub(super) fn parse_string_object(
    payload: &[u8],
) -> Result<HashMap<String, String>, ClientControlError> {
    if payload.len() > CLIENT_MAX_CONTROL_BYTES {
        return Err(ClientControlError::InvalidJson);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ClientControlError::InvalidJson)?;
    let mut parser = JsonStringObjectParser {
        text: text.as_bytes(),
        offset: 0,
    };
    parser.skip_whitespace();
    parser.expect(b'{')?;
    parser.skip_whitespace();
    let mut fields = HashMap::new();
    if parser.consume(b'}') {
        parser.skip_whitespace();
        return if parser.offset == parser.text.len() {
            Ok(fields)
        } else {
            Err(ClientControlError::InvalidJson)
        };
    }
    loop {
        parser.skip_whitespace();
        let key = parser.parse_string()?;
        parser.skip_whitespace();
        parser.expect(b':')?;
        parser.skip_whitespace();
        let value = parser.parse_string()?;
        if fields.insert(key, value).is_some() {
            return Err(ClientControlError::DuplicateKey);
        }
        parser.skip_whitespace();
        if parser.consume(b'}') {
            parser.skip_whitespace();
            return if parser.offset == parser.text.len() {
                Ok(fields)
            } else {
                Err(ClientControlError::InvalidJson)
            };
        }
        parser.expect(b',')?;
    }
}

pub(crate) struct JsonStringObjectParser<'a> {
    pub(crate) text: &'a [u8],
    pub(crate) offset: usize,
}

impl JsonStringObjectParser<'_> {
    pub(crate) fn skip_whitespace(&mut self) {
        while self
            .text
            .get(self.offset)
            .is_some_and(|byte| matches!(byte, b' ' | b'\n' | b'\r' | b'\t'))
        {
            self.offset += 1;
        }
    }

    pub(crate) fn consume(&mut self, expected: u8) -> bool {
        if self.text.get(self.offset) == Some(&expected) {
            self.offset += 1;
            true
        } else {
            false
        }
    }

    pub(crate) fn expect(&mut self, expected: u8) -> Result<(), ClientControlError> {
        self.consume(expected)
            .then_some(())
            .ok_or(ClientControlError::InvalidJson)
    }

    pub(crate) fn parse_u64_number(&mut self) -> Result<u64, ClientControlError> {
        let start = self.offset;
        while self.text.get(self.offset).is_some_and(u8::is_ascii_digit) {
            self.offset += 1;
        }
        let digits = self
            .text
            .get(start..self.offset)
            .ok_or(ClientControlError::InvalidJson)?;
        if digits.is_empty() || (digits.len() > 1 && digits[0] == b'0') {
            return Err(ClientControlError::InvalidJson);
        }
        std::str::from_utf8(digits)
            .map_err(|_| ClientControlError::InvalidJson)?
            .parse()
            .map_err(|_| ClientControlError::InvalidJson)
    }

    pub(crate) fn parse_string(&mut self) -> Result<String, ClientControlError> {
        self.expect(b'"')?;
        let mut output = Zeroizing::new(Vec::new());
        loop {
            let byte = *self
                .text
                .get(self.offset)
                .ok_or(ClientControlError::InvalidJson)?;
            self.offset += 1;
            match byte {
                b'"' => {
                    let bytes = std::mem::take(&mut *output);
                    return String::from_utf8(bytes).map_err(|error| {
                        let mut bytes = error.into_bytes();
                        bytes.zeroize();
                        ClientControlError::InvalidJson
                    });
                }
                b'\\' => {
                    let escaped = *self
                        .text
                        .get(self.offset)
                        .ok_or(ClientControlError::InvalidJson)?;
                    self.offset += 1;
                    match escaped {
                        b'"' | b'\\' | b'/' => output.push(escaped),
                        b'b' => output.push(0x08),
                        b'f' => output.push(0x0c),
                        b'n' => output.push(b'\n'),
                        b'r' => output.push(b'\r'),
                        b't' => output.push(b'\t'),
                        b'u' => {
                            let first = self.parse_hex_quad()?;
                            let scalar = if (0xd800..=0xdbff).contains(&first) {
                                self.expect(b'\\')?;
                                self.expect(b'u')?;
                                let second = self.parse_hex_quad()?;
                                if !(0xdc00..=0xdfff).contains(&second) {
                                    return Err(ClientControlError::InvalidJson);
                                }
                                0x1_0000
                                    + (((first as u32 - 0xd800) << 10) | (second as u32 - 0xdc00))
                            } else if (0xdc00..=0xdfff).contains(&first) {
                                return Err(ClientControlError::InvalidJson);
                            } else {
                                first as u32
                            };
                            let character =
                                char::from_u32(scalar).ok_or(ClientControlError::InvalidJson)?;
                            let mut encoded = [0; 4];
                            output
                                .extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
                        }
                        _ => return Err(ClientControlError::InvalidJson),
                    }
                }
                0x00..=0x1f => return Err(ClientControlError::InvalidJson),
                other => output.push(other),
            }
        }
    }

    pub(crate) fn parse_boolean(&mut self) -> Result<bool, ClientControlError> {
        if self.text[self.offset..].starts_with(b"true") {
            self.offset += 4;
            Ok(true)
        } else if self.text[self.offset..].starts_with(b"false") {
            self.offset += 5;
            Ok(false)
        } else {
            Err(ClientControlError::InvalidJson)
        }
    }

    pub(super) fn parse_table_column_array(
        &mut self,
    ) -> Result<Vec<crate::storage_manager::TableColumnDefinition>, ClientControlError> {
        self.expect(b'[')?;
        self.skip_whitespace();
        let mut columns = Vec::new();
        if self.consume(b']') {
            return Ok(columns);
        }
        loop {
            if columns.len() >= crate::tuple::MAX_COLUMNS {
                return Err(ClientControlError::InvalidPrepare);
            }
            columns.push(self.parse_table_column_definition()?);
            self.skip_whitespace();
            if self.consume(b']') {
                break;
            }
            self.expect(b',')?;
        }
        Ok(columns)
    }

    pub(super) fn parse_string_array(&mut self) -> Result<Vec<String>, ClientControlError> {
        self.expect(b'[')?;
        self.skip_whitespace();
        let mut values = Vec::new();
        if self.consume(b']') {
            return Ok(values);
        }
        loop {
            if values.len() >= crate::tuple::MAX_COLUMNS {
                return Err(ClientControlError::InvalidPrepare);
            }
            values.push(self.parse_string()?);
            self.skip_whitespace();
            if self.consume(b']') {
                break;
            }
            self.expect(b',')?;
        }
        Ok(values)
    }

    pub(super) fn parse_string_array_array(
        &mut self,
    ) -> Result<Vec<Vec<String>>, ClientControlError> {
        self.expect(b'[')?;
        self.skip_whitespace();
        let mut values = Vec::new();
        if self.consume(b']') {
            return Ok(values);
        }
        loop {
            if values.len() >= crate::tuple::MAX_COLUMNS {
                return Err(ClientControlError::InvalidPrepare);
            }
            values.push(self.parse_string_array()?);
            self.skip_whitespace();
            if self.consume(b']') {
                break;
            }
            self.expect(b',')?;
        }
        Ok(values)
    }

    pub(super) fn parse_id_array(&mut self) -> Result<Vec<[u8; 16]>, ClientControlError> {
        self.expect(b'[')?;
        self.skip_whitespace();
        let mut ids = Vec::new();
        if self.consume(b']') {
            return Ok(ids);
        }
        loop {
            if ids.len() >= crate::tuple::MAX_COLUMNS {
                return Err(ClientControlError::InvalidPrepare);
            }
            ids.push(decode_lower_hex_id(&self.parse_string()?)?);
            self.skip_whitespace();
            if self.consume(b']') {
                break;
            }
            self.expect(b',')?;
        }
        Ok(ids)
    }

    pub(super) fn parse_id_array_allow_zero(
        &mut self,
    ) -> Result<Vec<[u8; 16]>, ClientControlError> {
        self.expect(b'[')?;
        self.skip_whitespace();
        let mut ids = Vec::new();
        if self.consume(b']') {
            return Ok(ids);
        }
        loop {
            if ids.len() >= crate::tuple::MAX_COLUMNS {
                return Err(ClientControlError::InvalidPrepare);
            }
            ids.push(decode_lower_hex_id_allow_zero(&self.parse_string()?)?);
            self.skip_whitespace();
            if self.consume(b']') {
                break;
            }
            self.expect(b',')?;
        }
        Ok(ids)
    }

    pub(super) fn parse_policy_recipient_array(
        &mut self,
    ) -> Result<Vec<crate::authorization::Subject>, ClientControlError> {
        self.expect(b'[')?;
        self.skip_whitespace();
        let mut recipients = Vec::new();
        if self.consume(b']') {
            return Ok(recipients);
        }
        loop {
            if recipients.len() >= crate::authorization::MAX_POLICY_SUBJECTS {
                return Err(ClientControlError::InvalidPrepare);
            }
            let value = self.parse_string()?;
            let (kind, id) = value
                .split_once(':')
                .ok_or(ClientControlError::InvalidPrepare)?;
            let id = decode_lower_hex_id(id)?;
            recipients.push(match kind {
                "user" => crate::authorization::Subject::Principal(id),
                "role" => crate::authorization::Subject::Role(id),
                _ => return Err(ClientControlError::InvalidPrepare),
            });
            self.skip_whitespace();
            if self.consume(b']') {
                break;
            }
            self.expect(b',')?;
        }
        Ok(recipients)
    }

    pub(super) fn parse_table_column_definition(
        &mut self,
    ) -> Result<crate::storage_manager::TableColumnDefinition, ClientControlError> {
        self.expect(b'{')?;
        let mut fields = std::collections::HashSet::new();
        let mut name = None;
        let mut scalar_type = None;
        let mut nullable = None;
        let mut generated_uuid_identity = None;
        let mut default = None;
        self.skip_whitespace();
        if self.consume(b'}') {
            return Err(ClientControlError::InvalidPrepare);
        }
        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            if !fields.insert(key.clone()) {
                return Err(ClientControlError::DuplicateKey);
            }
            self.skip_whitespace();
            self.expect(b':')?;
            self.skip_whitespace();
            match key.as_str() {
                "name" => name = Some(self.parse_string()?),
                "type" => scalar_type = Some(decode_table_scalar_type(&self.parse_string()?)?),
                "nullable" => nullable = Some(self.parse_boolean()?),
                "generated_uuid_identity" => generated_uuid_identity = Some(self.parse_boolean()?),
                "default" => default = Some(self.parse_table_column_default()?),
                _ => return Err(ClientControlError::UnknownKey),
            }
            self.skip_whitespace();
            if self.consume(b'}') {
                break;
            }
            self.expect(b',')?;
        }
        let name = name.ok_or(ClientControlError::MissingKey)?;
        let scalar_type = scalar_type.ok_or(ClientControlError::MissingKey)?;
        let generated_uuid_identity =
            generated_uuid_identity.ok_or(ClientControlError::MissingKey)?;
        if generated_uuid_identity && default.is_some() {
            return Err(ClientControlError::InvalidPrepare);
        }
        Ok(crate::storage_manager::TableColumnDefinition {
            name,
            scalar_type,
            nullable: nullable.ok_or(ClientControlError::MissingKey)?,
            generated_uuid_identity,
            default: default
                .map(|(kind, value)| decode_table_column_default(&kind, value, scalar_type))
                .transpose()?,
        })
    }

    pub(super) fn parse_table_column_default(
        &mut self,
    ) -> Result<(String, Option<String>), ClientControlError> {
        self.expect(b'{')?;
        let mut fields = std::collections::HashSet::new();
        let mut kind = None;
        let mut value = None;
        self.skip_whitespace();
        if self.consume(b'}') {
            return Err(ClientControlError::InvalidPrepare);
        }
        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            if !fields.insert(key.clone()) {
                return Err(ClientControlError::DuplicateKey);
            }
            self.skip_whitespace();
            self.expect(b':')?;
            self.skip_whitespace();
            match key.as_str() {
                "kind" => kind = Some(self.parse_string()?),
                "value" => value = Some(self.parse_string()?),
                _ => return Err(ClientControlError::UnknownKey),
            }
            self.skip_whitespace();
            if self.consume(b'}') {
                break;
            }
            self.expect(b',')?;
        }
        let kind = kind.ok_or(ClientControlError::MissingKey)?;
        if (kind == "literal" && (fields.len() != 2 || value.is_none()))
            || (kind != "literal" && (fields.len() != 1 || value.is_some()))
        {
            return Err(ClientControlError::InvalidPrepare);
        }
        Ok((kind, value))
    }

    pub(super) fn parse_hex_quad(&mut self) -> Result<u16, ClientControlError> {
        let end = self
            .offset
            .checked_add(4)
            .ok_or(ClientControlError::InvalidJson)?;
        let digits = self
            .text
            .get(self.offset..end)
            .ok_or(ClientControlError::InvalidJson)?;
        let mut value = 0u16;
        for digit in digits {
            value = value
                .checked_mul(16)
                .and_then(|value| value.checked_add(hex_nibble(*digit)? as u16))
                .ok_or(ClientControlError::InvalidJson)?;
        }
        self.offset = end;
        Ok(value)
    }
}

pub(crate) fn decode_table_scalar_type(
    text: &str,
) -> Result<crate::tuple::ScalarType, ClientControlError> {
    use crate::tuple::ScalarType;
    let invalid = || ClientControlError::InvalidPrepare;
    let parameter = |value: &str| -> Result<u32, ClientControlError> {
        if value.is_empty()
            || (value.len() > 1 && value.starts_with('0'))
            || !value.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(invalid());
        }
        value.parse().map_err(|_| invalid())
    };
    let scalar_type = match text {
        "boolean" => ScalarType::Boolean,
        "smallint" => ScalarType::SmallInt,
        "integer" => ScalarType::Integer,
        "bigint" => ScalarType::BigInt,
        "text" => ScalarType::Text,
        "bytea" => ScalarType::Bytea,
        "uuid" => ScalarType::Uuid,
        "date" => ScalarType::Date,
        "timestamptz" => ScalarType::TimestampTz,
        _ if text.starts_with("decimal(") && text.ends_with(')') => {
            let params = &text[8..text.len() - 1];
            let (precision, scale) = params.split_once(',').ok_or_else(invalid)?;
            if scale.contains(',') {
                return Err(invalid());
            }
            let precision = parameter(precision)?;
            let scale = parameter(scale)?;
            if !(1..=38).contains(&precision) || scale > precision {
                return Err(invalid());
            }
            ScalarType::Decimal {
                precision: precision as u8,
                scale: scale as u8,
            }
        }
        _ if text.starts_with("varchar(") && text.ends_with(')') => {
            let max_chars = parameter(&text[8..text.len() - 1])?;
            if max_chars == 0 {
                return Err(invalid());
            }
            ScalarType::Varchar { max_chars }
        }
        _ => return Err(invalid()),
    };
    Ok(scalar_type)
}

pub(super) fn decode_table_column_default(
    kind: &str,
    value: Option<String>,
    scalar_type: crate::tuple::ScalarType,
) -> Result<crate::storage_manager::TableColumnDefault, ClientControlError> {
    use crate::storage_manager::TableColumnDefault;
    use crate::tuple::{ScalarType, ScalarValue};
    let invalid = || ClientControlError::InvalidPrepare;
    match (kind, value) {
        ("null", None) => Ok(TableColumnDefault::Literal(None)),
        ("current_principal_id", None) if scalar_type == ScalarType::Uuid => {
            Ok(TableColumnDefault::CurrentPrincipalId)
        }
        ("literal", Some(text)) => {
            let value = match scalar_type {
                ScalarType::Boolean => ScalarValue::Boolean(match text.as_str() {
                    "true" => true,
                    "false" => false,
                    _ => return Err(invalid()),
                }),
                ScalarType::SmallInt => ScalarValue::SmallInt(parse_default_integer(&text)?),
                ScalarType::Integer => ScalarValue::Integer(parse_default_integer(&text)?),
                ScalarType::BigInt => ScalarValue::BigInt(parse_default_integer(&text)?),
                ScalarType::Decimal { scale, .. } => {
                    ScalarValue::Decimal(parse_default_decimal(&text, scale)?)
                }
                ScalarType::Text => ScalarValue::Text(text),
                ScalarType::Varchar { .. } => ScalarValue::Varchar(text),
                ScalarType::Bytea => ScalarValue::Bytea(decode_default_hex(&text)?),
                ScalarType::Uuid => {
                    ScalarValue::Uuid(decode_lower_hex_id(&text).map_err(|_| invalid())?)
                }
                ScalarType::Date => ScalarValue::Date(parse_default_integer(&text)?),
                ScalarType::TimestampTz => ScalarValue::TimestampTz(parse_default_integer(&text)?),
            };
            Ok(TableColumnDefault::Literal(Some(value)))
        }
        _ => Err(invalid()),
    }
}

pub(super) fn parse_default_integer<T>(text: &str) -> Result<T, ClientControlError>
where
    T: std::str::FromStr,
{
    let digits = text.strip_prefix('-').unwrap_or(text);
    if digits.is_empty()
        || (digits.len() > 1 && digits.starts_with('0'))
        || text == "-0"
        || !digits.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    text.parse().map_err(|_| ClientControlError::InvalidPrepare)
}

pub(super) fn parse_default_decimal(text: &str, scale: u8) -> Result<i128, ClientControlError> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    let (whole, fraction) = match unsigned.split_once('.') {
        Some(parts) => parts,
        None if scale == 0 => (unsigned, ""),
        None => return Err(ClientControlError::InvalidPrepare),
    };
    if whole.is_empty()
        || (whole.len() > 1 && whole.starts_with('0'))
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() != scale as usize
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || text == "-0"
        || (negative && whole == "0" && fraction.bytes().all(|byte| byte == b'0'))
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    let digits = format!("{whole}{fraction}");
    let magnitude = digits
        .parse::<i128>()
        .map_err(|_| ClientControlError::InvalidPrepare)?;
    if negative {
        magnitude
            .checked_neg()
            .ok_or(ClientControlError::InvalidPrepare)
    } else {
        Ok(magnitude)
    }
}

pub(super) fn decode_default_hex(text: &str) -> Result<Vec<u8>, ClientControlError> {
    if !text.len().is_multiple_of(2)
        || !text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_nibble(pair[0]).ok_or(ClientControlError::InvalidPrepare)?;
            let low = hex_nibble(pair[1]).ok_or(ClientControlError::InvalidPrepare)?;
            Ok((high << 4) | low)
        })
        .collect()
}

pub(super) fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(super) fn take_required(
    fields: &mut HashMap<String, String>,
    key: &str,
) -> Result<String, ClientControlError> {
    fields.remove(key).ok_or(ClientControlError::MissingKey)
}

pub(super) fn reject_unknown_fields(
    fields: HashMap<String, String>,
) -> Result<(), ClientControlError> {
    if fields.is_empty() {
        Ok(())
    } else {
        Err(ClientControlError::UnknownKey)
    }
}

pub(super) fn reject_unknown_scalars(
    fields: HashMap<String, JsonScalar>,
) -> Result<(), ClientControlError> {
    if fields.is_empty() {
        Ok(())
    } else {
        Err(ClientControlError::UnknownKey)
    }
}

pub(super) fn decode_lower_hex_id(text: &str) -> Result<[u8; 16], ClientControlError> {
    let id = decode_lower_hex_id_allow_zero(text)?;
    if id == [0; 16] {
        return Err(ClientControlError::InvalidTokenId);
    }
    Ok(id)
}

pub(super) fn decode_lower_hex_id_allow_zero(text: &str) -> Result<[u8; 16], ClientControlError> {
    if text.len() != 32 {
        return Err(ClientControlError::InvalidTokenId);
    }
    let mut id = [0; 16];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        let high = lower_hex_nibble(pair[0]).ok_or(ClientControlError::InvalidTokenId)?;
        let low = lower_hex_nibble(pair[1]).ok_or(ClientControlError::InvalidTokenId)?;
        id[index] = (high << 4) | low;
    }
    Ok(id)
}

pub(super) fn decode_lower_hex_bytes(
    text: &str,
    max_bytes: usize,
) -> Result<Vec<u8>, ClientControlError> {
    if text.is_empty() || !text.len().is_multiple_of(2) || text.len() / 2 > max_bytes {
        return Err(ClientControlError::InvalidPrepare);
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(text.len() / 2)
        .map_err(|_| ClientControlError::InvalidPrepare)?;
    for pair in text.as_bytes().chunks_exact(2) {
        let high = lower_hex_nibble(pair[0]).ok_or(ClientControlError::InvalidPrepare)?;
        let low = lower_hex_nibble(pair[1]).ok_or(ClientControlError::InvalidPrepare)?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

pub(super) fn decode_metadata_pagination(
    arguments: &mut HashMap<String, String>,
) -> Result<Option<MetadataPagination>, ClientControlError> {
    let Some(limit) = arguments.remove("limit") else {
        return if arguments.contains_key("cursor") {
            Err(ClientControlError::InvalidMetadataCursor)
        } else {
            Ok(None)
        };
    };
    let page_limit = limit
        .parse::<usize>()
        .ok()
        .filter(|value| {
            (1..=CLIENT_METADATA_MAX_PAGE).contains(value) && value.to_string() == limit
        })
        .ok_or(ClientControlError::InvalidMetadataPageLimit)?;
    let cursor = arguments
        .remove("cursor")
        .map(|encoded| {
            let cursor = decode_lower_hex_id(&encoded)
                .map_err(|_| ClientControlError::InvalidMetadataCursor)?;
            if cursor[0] != CLIENT_METADATA_CURSOR_KIND {
                return Err(ClientControlError::InvalidMetadataCursor);
            }
            Ok(cursor)
        })
        .transpose()?;
    Ok(Some(MetadataPagination { page_limit, cursor }))
}

pub(super) fn lower_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

pub(super) fn decode_canonical_base64(
    text: &str,
) -> Result<Zeroizing<Vec<u8>>, ClientControlError> {
    let input = text.as_bytes();
    if input.is_empty() || !input.len().is_multiple_of(4) {
        return Err(ClientControlError::InvalidTokenSecret);
    }
    let mut output = Zeroizing::new(Vec::new());
    output
        .try_reserve_exact(input.len() / 4 * 3)
        .map_err(|_| ClientControlError::InvalidTokenSecret)?;
    for (index, chunk) in input.chunks_exact(4).enumerate() {
        let last = index + 1 == input.len() / 4;
        let a = base64_value(chunk[0]).ok_or(ClientControlError::InvalidTokenSecret)?;
        let b = base64_value(chunk[1]).ok_or(ClientControlError::InvalidTokenSecret)?;
        output.push((a << 2) | (b >> 4));
        match (chunk[2], chunk[3]) {
            (b'=', b'=') if last && b & 0x0f == 0 => (),
            (c, b'=') if last => {
                let c = base64_value(c).ok_or(ClientControlError::InvalidTokenSecret)?;
                if c & 0x03 != 0 {
                    return Err(ClientControlError::InvalidTokenSecret);
                }
                output.push((b << 4) | (c >> 2));
            }
            (c, d) if c != b'=' && d != b'=' => {
                let c = base64_value(c).ok_or(ClientControlError::InvalidTokenSecret)?;
                let d = base64_value(d).ok_or(ClientControlError::InvalidTokenSecret)?;
                output.push((b << 4) | (c >> 2));
                output.push((c << 6) | d);
            }
            _ => return Err(ClientControlError::InvalidTokenSecret),
        }
    }
    if output.len() != TOKEN_SECRET_BYTES {
        return Err(ClientControlError::InvalidTokenSecret);
    }
    Ok(output)
}

pub(super) fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

pub(super) fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let a = chunk[0];
        let b = *chunk.get(1).unwrap_or(&0);
        let c = *chunk.get(2).unwrap_or(&0);
        output.push(ALPHABET[(a >> 2) as usize] as char);
        output.push(ALPHABET[(((a & 0x03) << 4) | (b >> 4)) as usize] as char);
        if chunk.len() > 1 {
            output.push(ALPHABET[(((b & 0x0f) << 2) | (c >> 6)) as usize] as char);
        } else {
            output.push('=');
        }
        if chunk.len() > 2 {
            output.push(ALPHABET[(c & 0x3f) as usize] as char);
        } else {
            output.push('=');
        }
    }
    output
}

pub(super) fn encode_lower_hex_id(id: [u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(32);
    for byte in id {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

pub(super) fn encode_lower_hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

pub(super) fn push_json_string(output: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            c if c <= '\u{1f}' => {
                use std::fmt::Write as _;
                let _ = write!(output, "\\u{:04x}", c as u32);
            }
            c => output.push(c),
        }
    }
}

pub(super) fn push_json_optional_i64(output: &mut String, value: Option<i64>) {
    if let Some(value) = value {
        output.push_str(&value.to_string());
    } else {
        output.push_str("null");
    }
}

pub(super) struct RuntimeInvocationSummary<'a> {
    pub(super) pinned_snapshot_count: usize,
    pub(super) oldest_snapshot_pin_age_us: u64,
    pub(super) invocations_admitted_total: u64,
    pub(super) invocation_conflicts_total: u64,
    pub(super) admin_cancellations_accepted_total: u64,
    pub(super) admin_cancellations_denied_total: u64,
    pub(super) invocation_policy_denials_total: u64,
    pub(super) invocation_staged_value_bytes_total: u64,
    pub(super) invocation_commit_latency_buckets: [u64; 6],
    pub(super) invocation_redb_write_acquisition_buckets: [u64; 6],
    pub(super) include_write_lock_wait: bool,
    pub(super) include_outcome_maintenance: bool,
    pub(super) outcome_maintenance_passes_total: u64,
    pub(super) outcome_maintenance_failures_total: u64,
    pub(super) outcomes_pruned_total: u64,
    pub(super) waits: &'a [crate::ddl_fence::DdlFenceWaitStatus],
    pub(super) truncated: bool,
}

pub(super) fn encode_fence_waits(output: &mut String, summary: RuntimeInvocationSummary<'_>) {
    output.push_str("],\"pinned_snapshot_count\":");
    output.push_str(&summary.pinned_snapshot_count.to_string());
    output.push_str(",\"oldest_snapshot_pin_age_us\":");
    output.push_str(&summary.oldest_snapshot_pin_age_us.to_string());
    output.push_str(",\"invocations_admitted_total\":");
    output.push_str(&summary.invocations_admitted_total.to_string());
    output.push_str(",\"invocation_conflicts_total\":");
    output.push_str(&summary.invocation_conflicts_total.to_string());
    output.push_str(",\"admin_cancellations_accepted_total\":");
    output.push_str(&summary.admin_cancellations_accepted_total.to_string());
    output.push_str(",\"admin_cancellations_denied_total\":");
    output.push_str(&summary.admin_cancellations_denied_total.to_string());
    output.push_str(",\"invocation_policy_denials_total\":");
    output.push_str(&summary.invocation_policy_denials_total.to_string());
    output.push_str(",\"invocation_staged_value_bytes_total\":");
    output.push_str(&summary.invocation_staged_value_bytes_total.to_string());
    output.push_str(",\"invocation_commit_latency_buckets\":[");
    for (index, bucket) in summary.invocation_commit_latency_buckets.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        output.push_str(&bucket.to_string());
    }
    output.push(']');
    if summary.include_write_lock_wait {
        output.push_str(",\"invocation_redb_write_acquisition_buckets\":[");
        for (index, bucket) in summary
            .invocation_redb_write_acquisition_buckets
            .iter()
            .enumerate()
        {
            if index != 0 {
                output.push(',');
            }
            output.push_str(&bucket.to_string());
        }
        output.push(']');
    }
    if summary.include_outcome_maintenance {
        output.push_str(",\"outcome_maintenance_passes_total\":");
        output.push_str(&summary.outcome_maintenance_passes_total.to_string());
        output.push_str(",\"outcome_maintenance_failures_total\":");
        output.push_str(&summary.outcome_maintenance_failures_total.to_string());
        output.push_str(",\"outcomes_pruned_total\":");
        output.push_str(&summary.outcomes_pruned_total.to_string());
    }
    output.push_str(",\"fence_waits\":[");
    for (index, wait) in summary.waits.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        output.push_str("{\"wait_id\":\"");
        output.push_str(&encode_lower_hex_id(wait.wait_id));
        output.push_str("\",\"category\":\"");
        output.push_str(match wait.category {
            crate::ddl_fence::DdlFenceWaitCategory::Writer => "writer",
            crate::ddl_fence::DdlFenceWaitCategory::Ddl => "ddl",
        });
        output.push_str("\",\"waited_us\":");
        output.push_str(&wait.waited_us.to_string());
        output.push_str(" ,\"resource_ids\":[");
        for (resource_index, resource_id) in wait.resource_ids.iter().enumerate() {
            if resource_index != 0 {
                output.push(',');
            }
            output.push('\"');
            output.push_str(&encode_lower_hex_id(*resource_id));
            output.push('\"');
        }
        output.push_str("],\"resources_truncated\":");
        output.push_str(if wait.resources_truncated {
            "true"
        } else {
            "false"
        });
        output.push_str(" ,\"blocker_ids\":[");
        for (blocker_index, blocker_id) in wait.blocker_ids.iter().enumerate() {
            if blocker_index != 0 {
                output.push(',');
            }
            output.push('\"');
            output.push_str(&encode_lower_hex_id(*blocker_id));
            output.push('\"');
        }
        output.push_str("],\"blockers_truncated\":");
        output.push_str(if wait.blockers_truncated {
            "true"
        } else {
            "false"
        });
        output.push('}');
    }
    output.push_str("],\"fence_waits_truncated\":");
    output.push_str(if summary.truncated { "true" } else { "false" });
}

pub(super) fn encode_sdk_library_page(
    releases: &[crate::sdk_registry::SdkReleaseInfo],
    next_cursor: Option<[u8; 16]>,
) -> String {
    let mut payload = String::from("{\"sdk_libraries\":[");
    for (index, release) in releases.iter().enumerate() {
        if index != 0 {
            payload.push(',');
        }
        payload.push_str("{\"release\":");
        payload.push_str(&release.sdk_release.to_string());
        payload.push_str(",\"native_abis\":[");
        for (abi_index, abi) in release.native_abis.iter().enumerate() {
            if abi_index != 0 {
                payload.push(',');
            }
            payload.push_str(&abi.to_string());
        }
        payload.push_str("],\"target\":\"");
        push_json_string(&mut payload, &release.target);
        payload.push_str("\",\"cpu_baseline\":\"");
        push_json_string(&mut payload, &release.cpu_baseline);
        payload.push_str("\",\"runtime_profile\":\"");
        push_json_string(&mut payload, &release.runtime_profile);
        payload.push_str("\",\"soname\":\"");
        push_json_string(&mut payload, &release.soname);
        payload.push_str("\",\"sha256\":\"");
        payload.push_str(&encode_lower_hex_bytes(&release.sha256));
        payload.push_str("\",\"needed\":[");
        for (dependency_index, dependency) in release.needed.iter().enumerate() {
            if dependency_index != 0 {
                payload.push(',');
            }
            payload.push('"');
            push_json_string(&mut payload, dependency);
            payload.push('"');
        }
        payload.push_str("],\"library_bytes\":");
        payload.push_str(&release.library_bytes.to_string());
        payload.push('}');
    }
    payload.push_str("],\"next_cursor\":");
    match next_cursor {
        Some(cursor) => {
            payload.push('"');
            payload.push_str(&encode_lower_hex_id(cursor));
            payload.push('"');
        }
        None => payload.push_str("null"),
    }
    payload.push('}');
    payload
}

pub(super) fn encode_metadata_page(
    snapshot: &ActiveMetadataPageSnapshot,
    start: usize,
    end: usize,
    next_cursor: Option<[u8; 16]>,
) -> Result<Vec<u8>, ClientSessionError> {
    let mut output = match &snapshot.query {
        MetadataPageQuery::Schemas => String::from("{\"schemas\":["),
        MetadataPageQuery::Relations(_) => String::from("{\"relations\":["),
        MetadataPageQuery::Columns { .. } => String::from("{\"columns\":["),
        MetadataPageQuery::Indexes { .. } => String::from("{\"indexes\":["),
        MetadataPageQuery::Constraints { .. } => String::from("{\"constraints\":["),
        MetadataPageQuery::Grants { .. } => String::from("{\"grants\":["),
        MetadataPageQuery::Policies { .. } => String::from("{\"policies\":["),
        MetadataPageQuery::Dependencies(_) => String::from("{\"dependencies\":["),
    };
    for index in start..end {
        if index != start {
            output.push(',');
        }
        match (&snapshot.query, &snapshot.rows) {
            (MetadataPageQuery::Schemas, MetadataPageRows::Schemas(rows)) => {
                let row = rows.get(index).ok_or(ClientSessionError::ResourceLimit)?;
                output.push_str("{\"schema_id\":\"");
                output.push_str(&encode_lower_hex_id(row.schema_id));
                output.push_str("\",\"name\":\"");
                push_json_string(&mut output, &row.name);
                output.push_str("\"}");
            }
            (MetadataPageQuery::Relations(_), MetadataPageRows::Relations(rows)) => {
                let row = rows.get(index).ok_or(ClientSessionError::ResourceLimit)?;
                output.push_str("{\"relation_id\":\"");
                output.push_str(&encode_lower_hex_id(row.relation_id));
                output.push_str("\",\"name\":\"");
                push_json_string(&mut output, &row.name);
                output.push_str("\"}");
            }
            (MetadataPageQuery::Columns { .. }, MetadataPageRows::Columns(rows)) => {
                let row = rows.get(index).ok_or(ClientSessionError::ResourceLimit)?;
                output.push_str("{\"column_id\":\"");
                output.push_str(&encode_lower_hex_id(row.column_id));
                output.push_str("\",\"name\":\"");
                push_json_string(&mut output, &row.name);
                output.push_str("\",\"ordinal\":");
                output.push_str(&row.ordinal.to_string());
                output.push_str(" ,\"scalar_type\":\"");
                push_json_string(
                    &mut output,
                    &crate::metadata_discovery::scalar_type_name(row.scalar_type),
                );
                output.push_str("\",\"nullable\":");
                output.push_str(if row.nullable { "true" } else { "false" });
                output.push('}');
            }
            (MetadataPageQuery::Indexes { .. }, MetadataPageRows::Indexes(rows)) => {
                let row = rows.get(index).ok_or(ClientSessionError::ResourceLimit)?;
                output.push_str("{\"index_id\":\"");
                output.push_str(&encode_lower_hex_id(row.index_id));
                output.push_str("\",\"name\":\"");
                push_json_string(&mut output, &row.name);
                output.push_str("\",\"column_ids\":[");
                for (column_index, column_id) in row.column_ids.iter().enumerate() {
                    if column_index != 0 {
                        output.push(',');
                    }
                    output.push('"');
                    output.push_str(&encode_lower_hex_id(*column_id));
                    output.push('"');
                }
                output.push_str("],\"unique\":");
                output.push_str(if row.unique { "true" } else { "false" });
                output.push('}');
            }
            (MetadataPageQuery::Constraints { .. }, MetadataPageRows::Constraints(rows)) => {
                let row = rows.get(index).ok_or(ClientSessionError::ResourceLimit)?;
                output.push_str("{\"constraint_id\":\"");
                output.push_str(&encode_lower_hex_id(row.constraint_id));
                output.push_str("\",\"name\":");
                output.push('"');
                push_json_string(&mut output, &row.name);
                output.push('"');
                output.push_str(",\"kind\":\"");
                output.push_str(row.kind.as_wire_name());
                output.push_str("\",\"column_ids\":[");
                for (column_index, column_id) in row.column_ids.iter().enumerate() {
                    if column_index != 0 {
                        output.push(',');
                    }
                    output.push('"');
                    output.push_str(&encode_lower_hex_id(*column_id));
                    output.push('"');
                }
                output.push_str("],\"referenced_table_id\":");
                if let Some(table_id) = row.referenced_table_id {
                    output.push('"');
                    output.push_str(&encode_lower_hex_id(table_id));
                    output.push('"');
                } else {
                    output.push_str("null");
                }
                output.push_str(" ,\"referenced_column_ids\":[");
                for (column_index, column_id) in row.referenced_column_ids.iter().enumerate() {
                    if column_index != 0 {
                        output.push(',');
                    }
                    output.push('"');
                    output.push_str(&encode_lower_hex_id(*column_id));
                    output.push('"');
                }
                output.push_str("]}");
            }
            (MetadataPageQuery::Grants { .. }, MetadataPageRows::Grants(rows)) => {
                let row = rows.get(index).ok_or(ClientSessionError::ResourceLimit)?;
                output.push_str("{\"grantee_id\":\"");
                output.push_str(&encode_lower_hex_id(row.grantee_id));
                output.push_str("\",\"grantor_id\":\"");
                output.push_str(&encode_lower_hex_id(row.grantor_id));
                output.push_str("\",\"privilege\":\"");
                output.push_str(row.privilege.as_wire_name());
                output.push_str("\",\"target_scope\":\"");
                output.push_str(row.target_scope.as_wire_name());
                output.push_str("\",\"target_id\":\"");
                output.push_str(&encode_lower_hex_id(row.target_id));
                output.push_str("\"}");
            }
            (MetadataPageQuery::Policies { .. }, MetadataPageRows::Policies(rows)) => {
                let row = rows.get(index).ok_or(ClientSessionError::ResourceLimit)?;
                output.push_str("{\"policy_id\":\"");
                output.push_str(&encode_lower_hex_id(row.policy_id));
                output.push_str("\",\"kind\":\"");
                output.push_str(match row.kind {
                    crate::metadata_discovery::AdministrativePolicyKind::Row => "row",
                    crate::metadata_discovery::AdministrativePolicyKind::Cell => "cell",
                });
                output.push_str("\",\"operation\":\"");
                output.push_str(match row.operation {
                    crate::metadata_discovery::AdministrativePolicyOperation::Select => "select",
                    crate::metadata_discovery::AdministrativePolicyOperation::Insert => "insert",
                    crate::metadata_discovery::AdministrativePolicyOperation::Update => "update",
                    crate::metadata_discovery::AdministrativePolicyOperation::Delete => "delete",
                });
                output.push_str("\",\"effect\":\"");
                output.push_str(match row.effect {
                    crate::metadata_discovery::AdministrativePolicyEffect::Allow => "allow",
                    crate::metadata_discovery::AdministrativePolicyEffect::Restrict => "restrict",
                });
                output.push_str("\",\"column_ids\":[");
                for (column_index, column_id) in row.column_ids.iter().enumerate() {
                    if column_index != 0 {
                        output.push(',');
                    }
                    output.push('"');
                    output.push_str(&encode_lower_hex_id(*column_id));
                    output.push('"');
                }
                output.push_str("],\"recipients\":");
                if let Some(recipients) = &row.recipients {
                    output.push('[');
                    for (recipient_index, recipient) in recipients.iter().enumerate() {
                        if recipient_index != 0 {
                            output.push(',');
                        }
                        output.push_str("{\"kind\":\"");
                        output.push_str(match recipient.kind {
                            crate::metadata_discovery::AdministrativePolicyRecipientKind::User => {
                                "user"
                            }
                            crate::metadata_discovery::AdministrativePolicyRecipientKind::Role => {
                                "role"
                            }
                        });
                        output.push_str("\",\"principal_id\":\"");
                        output.push_str(&encode_lower_hex_id(recipient.principal_id));
                        output.push_str("\"}");
                    }
                    output.push(']');
                } else {
                    output.push_str("null");
                }
                output.push('}');
            }
            (MetadataPageQuery::Dependencies(_), MetadataPageRows::Dependencies(rows)) => {
                let row = rows.get(index).ok_or(ClientSessionError::ResourceLimit)?;
                output.push_str("{\"dependent_id\":\"");
                output.push_str(&encode_lower_hex_id(row.dependent_id));
                output.push_str("\",\"referenced_id\":\"");
                output.push_str(&encode_lower_hex_id(row.referenced_id));
                output.push_str("\",\"kind\":\"");
                output.push_str(row.kind.as_wire_name());
                output.push_str("\"}");
            }
            _ => return Err(ClientSessionError::ResourceLimit),
        }
    }
    output.push_str("],\"next_cursor\":");
    match next_cursor {
        Some(cursor) => {
            output.push('"');
            output.push_str(&encode_lower_hex_id(cursor));
            output.push('"');
        }
        None => output.push_str("null"),
    }
    output.push('}');
    Ok(output.into_bytes())
}

pub(super) fn current_unix_micros() -> Option<i64> {
    let duration = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?;
    i64::try_from(duration.as_micros()).ok()
}
