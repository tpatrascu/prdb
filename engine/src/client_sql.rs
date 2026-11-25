//! Local SQL input and PSP1 parameter packaging for the interactive SQL client.

use crate::client_submit::{ResponseValue, parse_bounded_json_value};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{self, Read};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use zeroize::Zeroizing;

pub const MAX_SQL_BYTES: usize = 65_536;
pub const MAX_SQL_PARAMETER_BYTES: usize = 1024 * 1024;
pub const MAX_SQL_PARAMETER_COUNT: usize = u16::MAX as usize;
const PSP1_HEADER_BYTES: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqlInputError {
    InvalidFormat,
    InvalidSql,
    InvalidSchema,
    UnsupportedInteractiveStatement,
    TooLarge,
    Io,
}

impl std::fmt::Display for SqlInputError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidFormat => "invalid SQL parameter file",
            Self::InvalidSql => "SQL input must be nonempty UTF-8 without NUL bytes",
            Self::InvalidSchema => "schema must be a normalized identifier",
            Self::UnsupportedInteractiveStatement => {
                "interactive batch contains a statement that cannot be packaged as a query"
            }
            Self::TooLarge => "SQL input or parameters exceed their size limit",
            Self::Io => "could not read SQL input",
        };
        formatter.write_str(message)
    }
}

impl std::error::Error for SqlInputError {}

pub fn read_sql_input(path: &Path) -> Result<Vec<u8>, SqlInputError> {
    if path == Path::new("-") {
        return read_sql_from(&mut io::stdin().lock());
    }
    let file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| SqlInputError::Io)?;
    if !file.metadata().map_err(|_| SqlInputError::Io)?.is_file() {
        return Err(SqlInputError::InvalidSql);
    }
    read_sql_from(file)
}

pub fn read_sql_from(mut input: impl Read) -> Result<Vec<u8>, SqlInputError> {
    let mut bytes = Vec::new();
    input
        .by_ref()
        .take((MAX_SQL_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| SqlInputError::Io)?;
    if bytes.len() > MAX_SQL_BYTES {
        bytes.fill(0);
        return Err(SqlInputError::TooLarge);
    }
    let text = std::str::from_utf8(&bytes).map_err(|_| SqlInputError::InvalidSql)?;
    if text.is_empty() || text.bytes().all(|byte| byte.is_ascii_whitespace()) || bytes.contains(&0)
    {
        bytes.fill(0);
        return Err(SqlInputError::InvalidSql);
    }
    Ok(bytes)
}

pub fn validate_schema_name(schema: &str) -> Result<(), SqlInputError> {
    if schema.is_empty()
        || schema.len() > 128
        || !schema.as_bytes()[0].is_ascii_lowercase() && schema.as_bytes()[0] != b'_'
        || !schema
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(SqlInputError::InvalidSchema);
    }
    Ok(())
}

/// Generate a fixed C wrapper for a lexed sequence of ordinary query or DML
/// statements. Transaction control, DDL, and RUNAS blocks are handled by the
/// higher-level dispatcher and cannot enter this package helper.
pub fn generate_query_batch_source(schema: &str, sql: &[u8]) -> Result<Vec<u8>, SqlInputError> {
    validate_schema_name(schema)?;
    if sql.is_empty() || sql.len() > MAX_SQL_BYTES || sql.contains(&0) {
        return Err(SqlInputError::InvalidSql);
    }
    let sql_text = std::str::from_utf8(sql).map_err(|_| SqlInputError::InvalidSql)?;
    let parsed = crate::interactive_sql::parse_interactive_sql(sql_text)
        .map_err(|_| SqlInputError::InvalidSql)?;
    if parsed.is_empty() || parsed.len() > crate::interactive_sql::MAX_RUNAS_STATEMENTS {
        return Err(SqlInputError::TooLarge);
    }
    let queries: Vec<(&str, bool)> = parsed
        .iter()
        .map(|statement| match statement {
            crate::interactive_sql::InteractiveStatement::Query(query) => Ok((
                *query,
                crate::interactive_sql::contains_parameter_placeholder(query),
            )),
            _ => Err(SqlInputError::UnsupportedInteractiveStatement),
        })
        .collect::<Result<_, _>>()?;

    let mut source = String::from(
        "#include <stddef.h>\n#include \"prdb_query_abi_v1.h\"\n#include \"prdb_sdk_sql_dispatch_v1.h\"\n#include \"prdb_sdk_sql_batch_v1.h\"\n\n",
    );
    source.push_str("static const uint8_t sql_schema[] = {");
    append_c_bytes(&mut source, schema.as_bytes());
    source.push_str("};\n");
    for (index, (query, _)) in queries.iter().enumerate() {
        use std::fmt::Write as _;
        let _ = write!(source, "static const uint8_t sql_text_{index}[] = {{");
        append_c_bytes(&mut source, query.as_bytes());
        source.push_str("};\n");
    }
    source.push_str("\nstatic int32_t dispatch_batch_statement(const prdb_host_api_v1 *host, const uint8_t *schema, uint32_t schema_len, const prdb_sdk_sql_batch_statement_v1 *statement) {\n    int32_t status = prdb_sdk_sql_run_statement_v1(host, schema, schema_len, statement->sql, statement->sql_len, statement->parameters, statement->parameters_len);\n    return status == PRDB_SDK_SQL_DISPATCH_OK ? 0 : status;\n}\n\n");
    source.push_str("const prdb_query_descriptor_v1_record prdb_query_descriptor_v1 = { sizeof(prdb_query_descriptor_v1_record), PRDB_QUERY_ABI_VERSION, sizeof(prdb_host_api_v1), sizeof(prdb_invocation_v1) };\n\nint32_t prdb_query_main_v1(const prdb_host_api_v1 *host, const prdb_invocation_v1 *invocation) {\n    if (invocation == NULL || invocation->struct_size < sizeof(*invocation) || invocation->abi_version != PRDB_QUERY_ABI_VERSION) return 1;\n    prdb_sdk_sql_batch_statement_v1 statements[] = {\n");
    for (index, (_, has_parameters)) in queries.iter().enumerate() {
        use std::fmt::Write as _;
        if *has_parameters {
            let _ = writeln!(
                source,
                "        {{sql_text_{index}, sizeof(sql_text_{index}), invocation->parameters, invocation->parameters_len}},"
            );
        } else {
            let _ = writeln!(
                source,
                "        {{sql_text_{index}, sizeof(sql_text_{index}), NULL, 0}},"
            );
        }
    }
    use std::fmt::Write as _;
    let _ = writeln!(
        source,
        "    }};\n    return prdb_sdk_sql_run_batch_v1(host, sql_schema, sizeof(sql_schema), {}, statements, dispatch_batch_statement) == PRDB_SDK_SQL_BATCH_OK ? 0 : 1;\n}}",
        queries.len()
    );
    Ok(source.into_bytes())
}

fn append_c_bytes(output: &mut String, bytes: &[u8]) {
    for (index, byte) in bytes.iter().enumerate() {
        if index != 0 {
            output.push(',');
        }
        use std::fmt::Write as _;
        let _ = write!(output, "0x{byte:02x}");
    }
}

pub fn encode_parameter_file(input: &[u8]) -> Result<Zeroizing<Vec<u8>>, SqlInputError> {
    if input.len() > MAX_SQL_PARAMETER_BYTES {
        return Err(SqlInputError::TooLarge);
    }
    let parsed = Zeroizing::new(
        parse_bounded_json_value(input, MAX_SQL_PARAMETER_BYTES)
            .map_err(|_| SqlInputError::InvalidFormat)?,
    );
    let root = parsed.as_object().ok_or(SqlInputError::InvalidFormat)?;
    if root.len() != 2 || string_field(root, "format")? != "PDB-SQL-PARAMETERS-1" {
        return Err(SqlInputError::InvalidFormat);
    }
    let values = root
        .get("values")
        .and_then(ResponseValue::as_array)
        .ok_or(SqlInputError::InvalidFormat)?;
    if values.len() > MAX_SQL_PARAMETER_COUNT {
        return Err(SqlInputError::TooLarge);
    }

    let mut records = Zeroizing::new(Vec::new());
    for value in values {
        let fields = value.as_object().ok_or(SqlInputError::InvalidFormat)?;
        if fields.len() != 2 {
            return Err(SqlInputError::InvalidFormat);
        }
        let type_name = string_field(fields, "type")?;
        let raw_value = fields.get("value").ok_or(SqlInputError::InvalidFormat)?;
        let (type_code, precision, scale, state, payload) =
            encode_typed_value(type_name, raw_value)?;
        let payload = Zeroizing::new(payload);
        let payload_len: u32 = payload
            .len()
            .try_into()
            .map_err(|_| SqlInputError::TooLarge)?;
        records.extend_from_slice(&type_code.to_be_bytes());
        records.push(precision);
        records.push(scale);
        records.push(state);
        records.push(0);
        records.extend_from_slice(&payload_len.to_be_bytes());
        records.extend_from_slice(payload.as_slice());
        if records.len() + PSP1_HEADER_BYTES > MAX_SQL_PARAMETER_BYTES {
            return Err(SqlInputError::TooLarge);
        }
    }

    let value_count: u16 = values
        .len()
        .try_into()
        .map_err(|_| SqlInputError::TooLarge)?;
    let mut output = Zeroizing::new(Vec::with_capacity(PSP1_HEADER_BYTES + records.len()));
    output.extend_from_slice(b"PSP1");
    output.extend_from_slice(&value_count.to_be_bytes());
    output.extend_from_slice(&0u16.to_be_bytes());
    output.extend_from_slice(records.as_slice());
    Ok(output)
}

fn string_field<'a>(
    fields: &'a HashMap<String, ResponseValue>,
    name: &str,
) -> Result<&'a str, SqlInputError> {
    fields
        .get(name)
        .and_then(ResponseValue::as_string)
        .ok_or(SqlInputError::InvalidFormat)
}

fn encode_typed_value(
    type_name: &str,
    value: &ResponseValue,
) -> Result<(u32, u8, u8, u8, Vec<u8>), SqlInputError> {
    let (type_code, precision, scale, type_kind) = parse_type(type_name)?;
    if matches!(value, ResponseValue::Null) {
        return Ok((type_code, precision, scale, 1, Vec::new()));
    }
    let text = value.as_string().ok_or(SqlInputError::InvalidFormat)?;
    let payload = match type_kind {
        SqlType::Boolean => match text {
            "false" => vec![0],
            "true" => vec![1],
            _ => return Err(SqlInputError::InvalidFormat),
        },
        SqlType::SmallInt => parse_signed::<i16>(text)?.to_le_bytes().to_vec(),
        SqlType::Integer => parse_signed::<i32>(text)?.to_le_bytes().to_vec(),
        SqlType::BigInt => parse_signed::<i64>(text)?.to_le_bytes().to_vec(),
        SqlType::Decimal => parse_decimal(text, precision, scale)?
            .to_le_bytes()
            .to_vec(),
        SqlType::Text => text.as_bytes().to_vec(),
        SqlType::Varchar(max_chars) => {
            if text.chars().count() > max_chars as usize {
                return Err(SqlInputError::InvalidFormat);
            }
            text.as_bytes().to_vec()
        }
        SqlType::Bytea => decode_bytea(text)?,
        SqlType::Uuid => decode_uuid(text)?.to_vec(),
        SqlType::Date => {
            let days = parse_signed::<i32>(text)?;
            if !(-719_162..=2_932_896).contains(&days) {
                return Err(SqlInputError::InvalidFormat);
            }
            days.to_le_bytes().to_vec()
        }
        SqlType::TimestampTz => {
            let micros = parse_signed::<i64>(text)?;
            if !(-62_135_596_800_000_000..=253_402_300_799_999_999).contains(&micros) {
                return Err(SqlInputError::InvalidFormat);
            }
            micros.to_le_bytes().to_vec()
        }
    };
    Ok((type_code, precision, scale, 0, payload))
}

#[derive(Clone, Copy)]
enum SqlType {
    Boolean,
    SmallInt,
    Integer,
    BigInt,
    Decimal,
    Text,
    Varchar(u32),
    Bytea,
    Uuid,
    Date,
    TimestampTz,
}

fn parse_type(text: &str) -> Result<(u32, u8, u8, SqlType), SqlInputError> {
    let parsed = match text {
        "boolean" => (1, 0, 0, SqlType::Boolean),
        "smallint" => (2, 0, 0, SqlType::SmallInt),
        "integer" => (3, 0, 0, SqlType::Integer),
        "bigint" => (4, 0, 0, SqlType::BigInt),
        "text" => (6, 0, 0, SqlType::Text),
        "bytea" => (8, 0, 0, SqlType::Bytea),
        "uuid" => (9, 0, 0, SqlType::Uuid),
        "date" => (10, 0, 0, SqlType::Date),
        "timestamptz" => (11, 0, 0, SqlType::TimestampTz),
        _ if text.starts_with("decimal(") && text.ends_with(')') => {
            let inside = &text[8..text.len() - 1];
            let (precision, scale) = inside.split_once(',').ok_or(SqlInputError::InvalidFormat)?;
            if scale.contains(',') {
                return Err(SqlInputError::InvalidFormat);
            }
            let precision = parse_unsigned::<u8>(precision)?;
            let scale = parse_unsigned::<u8>(scale)?;
            if !(1..=38).contains(&precision) || scale > precision {
                return Err(SqlInputError::InvalidFormat);
            }
            (5, precision, scale, SqlType::Decimal)
        }
        _ if text.starts_with("varchar(") && text.ends_with(')') => {
            let max_chars = parse_unsigned::<u32>(&text[8..text.len() - 1])?;
            if max_chars == 0 {
                return Err(SqlInputError::InvalidFormat);
            }
            (7, 0, 0, SqlType::Varchar(max_chars))
        }
        _ => return Err(SqlInputError::InvalidFormat),
    };
    Ok(parsed)
}

fn parse_unsigned<T: std::str::FromStr + ToString>(text: &str) -> Result<T, SqlInputError> {
    let value = text
        .parse::<T>()
        .map_err(|_| SqlInputError::InvalidFormat)?;
    if value.to_string() != text {
        return Err(SqlInputError::InvalidFormat);
    }
    Ok(value)
}

fn parse_signed<T: std::str::FromStr + ToString>(text: &str) -> Result<T, SqlInputError> {
    if text.is_empty() || text.starts_with('+') {
        return Err(SqlInputError::InvalidFormat);
    }
    let value = text
        .parse::<T>()
        .map_err(|_| SqlInputError::InvalidFormat)?;
    if value.to_string() != text {
        return Err(SqlInputError::InvalidFormat);
    }
    Ok(value)
}

fn parse_decimal(text: &str, precision: u8, scale: u8) -> Result<i128, SqlInputError> {
    let negative = text.starts_with('-');
    let unsigned = text.strip_prefix('-').unwrap_or(text);
    if unsigned.is_empty() {
        return Err(SqlInputError::InvalidFormat);
    }
    let (whole, fractional) = match unsigned.split_once('.') {
        Some((whole, fractional)) if !fractional.is_empty() => (whole, fractional),
        Some(_) => return Err(SqlInputError::InvalidFormat),
        None => (unsigned, ""),
    };
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
        || (whole.len() > 1 && whole.starts_with('0'))
        || fractional.len() > scale as usize
    {
        return Err(SqlInputError::InvalidFormat);
    }
    let mut coefficient = whole
        .parse::<i128>()
        .map_err(|_| SqlInputError::InvalidFormat)?
        .checked_mul(10i128.pow(scale as u32))
        .ok_or(SqlInputError::InvalidFormat)?;
    if !fractional.is_empty() {
        let fraction = fractional
            .parse::<i128>()
            .map_err(|_| SqlInputError::InvalidFormat)?;
        coefficient = coefficient
            .checked_add(fraction * 10i128.pow(scale as u32 - fractional.len() as u32))
            .ok_or(SqlInputError::InvalidFormat)?;
    }
    if negative {
        coefficient = coefficient
            .checked_neg()
            .ok_or(SqlInputError::InvalidFormat)?;
    }
    if coefficient == 0 && negative {
        return Err(SqlInputError::InvalidFormat);
    }
    let maximum = 10i128.pow(precision as u32);
    if coefficient.unsigned_abs() >= maximum as u128 {
        return Err(SqlInputError::InvalidFormat);
    }
    Ok(coefficient)
}

fn decode_bytea(text: &str) -> Result<Vec<u8>, SqlInputError> {
    let hex = text
        .strip_prefix("hex:")
        .ok_or(SqlInputError::InvalidFormat)?;
    if hex.len() % 2 != 0 {
        return Err(SqlInputError::InvalidFormat);
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks_exact(2) {
        bytes.push((lower_hex_nibble(pair[0])? << 4) | lower_hex_nibble(pair[1])?);
    }
    Ok(bytes)
}

fn decode_uuid(text: &str) -> Result<[u8; 16], SqlInputError> {
    if text.len() != 32 {
        return Err(SqlInputError::InvalidFormat);
    }
    let mut output = [0u8; 16];
    for (index, pair) in text.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (lower_hex_nibble(pair[0])? << 4) | lower_hex_nibble(pair[1])?;
    }
    Ok(output)
}

fn lower_hex_nibble(byte: u8) -> Result<u8, SqlInputError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(SqlInputError::InvalidFormat),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_SQL_BYTES, SqlInputError, encode_parameter_file, generate_query_batch_source,
        read_sql_from, validate_schema_name,
    };
    use crate::client_submit::ResponseValue;
    use std::collections::HashMap;
    use std::io::Cursor;
    use zeroize::Zeroize;

    #[test]
    fn parsed_parameter_json_tree_zeroizes_nested_strings() {
        let mut nested = HashMap::new();
        nested.insert(
            "private-field".to_owned(),
            ResponseValue::String("parameter-secret".to_owned()),
        );
        let mut fields = HashMap::new();
        fields.insert(
            "values".to_owned(),
            ResponseValue::Array(vec![
                ResponseValue::String("text".to_owned()),
                ResponseValue::Object(nested),
            ]),
        );
        let mut value = ResponseValue::Object(fields);

        value.zeroize();

        assert_eq!(value, ResponseValue::Object(HashMap::new()));
    }

    #[test]
    fn parameter_file_encodes_typed_values_and_null_as_psp1() {
        let input = br#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"integer","value":"42"},{"type":"text","value":null}]}"#;
        let expected = [
            b'P', b'S', b'P', b'1', 0, 2, 0, 0, // header
            0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 4, 42, 0, 0, 0, // integer 42
            0, 0, 0, 6, 0, 0, 1, 0, 0, 0, 0, 0, // typed NULL text
        ];
        assert_eq!(encode_parameter_file(input).unwrap().as_slice(), expected);
    }

    #[test]
    fn parameter_file_rejects_duplicate_keys_and_noncanonical_integer() {
        assert_eq!(
            encode_parameter_file(
                br#"{"format":"PDB-SQL-PARAMETERS-1","format":"PDB-SQL-PARAMETERS-1","values":[]}"#
            )
            .err(),
            Some(SqlInputError::InvalidFormat)
        );
        assert_eq!(
            encode_parameter_file(
                br#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"integer","value":"042"}]}"#
            )
            .err(),
            Some(SqlInputError::InvalidFormat)
        );
    }

    #[test]
    fn parameter_file_encodes_decimal_and_rejects_invalid_type_metadata() {
        let input = br#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"decimal(5,2)","value":"-12.3"}]}"#;
        let mut expected = vec![b'P', b'S', b'P', b'1', 0, 1, 0, 0];
        expected.extend_from_slice(&5u32.to_be_bytes());
        expected.extend_from_slice(&[5, 2, 0, 0]);
        expected.extend_from_slice(&16u32.to_be_bytes());
        expected.extend_from_slice(&(-1230i128).to_le_bytes());
        assert_eq!(encode_parameter_file(input).unwrap().as_slice(), expected);
        assert!(encode_parameter_file(
            br#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"decimal(2,3)","value":"1"}]}"#
        )
        .is_err());
    }

    #[test]
    fn parameter_file_encodes_every_scalar_type_as_canonical_psp1() {
        let input = r#"{"format":"PDB-SQL-PARAMETERS-1","values":[{"type":"boolean","value":"true"},{"type":"smallint","value":"-2"},{"type":"integer","value":"42"},{"type":"bigint","value":"-900000000000"},{"type":"decimal(5,2)","value":"-12.3"},{"type":"text","value":"é"},{"type":"varchar(2)","value":"🙂"},{"type":"bytea","value":"hex:00ff"},{"type":"uuid","value":"00112233445566778899aabbccddeeff"},{"type":"date","value":"-1"},{"type":"timestamptz","value":"1"}]}"#;
        let mut expected = b"PSP1\0\x0b\0\0".to_vec();
        let records: [(u32, u8, u8, Vec<u8>); 11] = [
            (1, 0, 0, vec![1]),
            (2, 0, 0, (-2i16).to_le_bytes().to_vec()),
            (3, 0, 0, 42i32.to_le_bytes().to_vec()),
            (4, 0, 0, (-900_000_000_000i64).to_le_bytes().to_vec()),
            (5, 5, 2, (-1230i128).to_le_bytes().to_vec()),
            (6, 0, 0, vec![0xc3, 0xa9]),
            (7, 0, 0, vec![0xf0, 0x9f, 0x99, 0x82]),
            (8, 0, 0, vec![0x00, 0xff]),
            (
                9,
                0,
                0,
                vec![
                    0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc,
                    0xdd, 0xee, 0xff,
                ],
            ),
            (10, 0, 0, (-1i32).to_le_bytes().to_vec()),
            (11, 0, 0, 1i64.to_le_bytes().to_vec()),
        ];
        for (type_code, precision, scale, payload) in records {
            expected.extend_from_slice(&type_code.to_be_bytes());
            expected.extend_from_slice(&[precision, scale, 0, 0]);
            expected.extend_from_slice(&(payload.len() as u32).to_be_bytes());
            expected.extend_from_slice(&payload);
        }

        assert_eq!(
            encode_parameter_file(input.as_bytes()).unwrap().as_slice(),
            expected
        );
    }

    #[test]
    fn parameter_file_rejects_noncanonical_or_out_of_range_scalar_values() {
        let invalid_values = [
            ("boolean", "TRUE"),
            ("smallint", "32768"),
            ("integer", "+1"),
            ("bigint", "01"),
            ("decimal(5,2)", "01.0"),
            ("decimal(5,2)", "-0"),
            ("decimal(5,2)", "1.234"),
            ("varchar(1)", "é🙂"),
            ("bytea", "hex:AF"),
            ("uuid", "00112233-4455-6677-8899-aabbccddeeff"),
            ("date", "2932897"),
            ("timestamptz", "+1"),
            ("timestamptz", "253402300800000000"),
        ];
        for (type_name, value) in invalid_values {
            let input = format!(
                "{{\"format\":\"PDB-SQL-PARAMETERS-1\",\"values\":[{{\"type\":\"{type_name}\",\"value\":\"{value}\"}}]}}"
            );
            assert_eq!(
                encode_parameter_file(input.as_bytes()).err(),
                Some(SqlInputError::InvalidFormat),
                "accepted invalid {type_name} value {value:?}"
            );
        }
    }

    #[test]
    fn sql_input_is_bounded_nonempty_utf8_and_nul_free() {
        let exact_limit = vec![b'x'; MAX_SQL_BYTES];
        assert_eq!(
            read_sql_from(Cursor::new(b"SELECT 1")).unwrap(),
            b"SELECT 1"
        );
        assert_eq!(
            read_sql_from(Cursor::new(&exact_limit)).unwrap(),
            exact_limit
        );
        assert_eq!(
            read_sql_from(Cursor::new(vec![b'x'; MAX_SQL_BYTES + 1])),
            Err(SqlInputError::TooLarge)
        );
        assert_eq!(
            read_sql_from(Cursor::new(b"SELECT\0 1")),
            Err(SqlInputError::InvalidSql)
        );
        assert_eq!(
            read_sql_from(Cursor::new([0xff])),
            Err(SqlInputError::InvalidSql)
        );
    }

    #[test]
    fn parameter_file_enforces_the_one_mibibyte_input_limit() {
        let oversized = vec![b' '; super::MAX_SQL_PARAMETER_BYTES + 1];
        assert_eq!(
            encode_parameter_file(&oversized).err(),
            Some(SqlInputError::TooLarge)
        );
    }

    #[test]
    fn schema_name_must_be_normalized() {
        assert!(validate_schema_name("public").is_ok());
        assert!(validate_schema_name("_stage2").is_ok());
        assert!(validate_schema_name("Public").is_err());
        assert!(validate_schema_name("schema-name").is_err());
    }

    #[test]
    fn generated_batch_wrapper_uses_lexer_boundaries_and_statement_ordinals() {
        let sql = b"SELECT name FROM employees; SELECT value FROM ledger;";
        let source = generate_query_batch_source("public", sql).unwrap();
        let source = std::str::from_utf8(&source).unwrap();
        assert!(source.contains("static const uint8_t sql_text_0[] = {"));
        assert!(source.contains("static const uint8_t sql_text_1[] = {"));
        assert!(source.contains("prdb_sdk_sql_run_batch_v1"));
        assert!(source.contains("prdb_sdk_sql_run_statement_v1"));
        assert!(source.contains("{sql_text_0, sizeof(sql_text_0), NULL, 0}"));
        assert!(source.contains("{sql_text_1, sizeof(sql_text_1), NULL, 0}"));
        assert!(!source.contains("SELECT name FROM employees"));
        assert!(!source.contains("SELECT value FROM ledger"));
        assert_eq!(
            generate_query_batch_source("public", b"SELECT name FROM t; BEGIN;"),
            Err(SqlInputError::UnsupportedInteractiveStatement)
        );
        assert_eq!(
            generate_query_batch_source("public", b"RUNAS reader\nSELECT name FROM t\nENDRUNAS"),
            Err(SqlInputError::UnsupportedInteractiveStatement)
        );
        let mixed_dml = generate_query_batch_source(
            "public",
            b"INSERT INTO t VALUES ($1); UPDATE t SET x = $2 WHERE id = $1;",
        )
        .unwrap();
        let mixed_dml = std::str::from_utf8(&mixed_dml).unwrap();
        assert!(mixed_dml.contains("prdb_sdk_sql_run_statement_v1"));
    }

    #[test]
    fn generated_batch_wrapper_passes_parameters_only_to_statements_that_reference_them() {
        let source = generate_query_batch_source(
            "public",
            b"INSERT INTO t VALUES ($1); SELECT '$1' FROM t; SELECT id FROM t -- $2\n",
        )
        .unwrap();
        let source = std::str::from_utf8(&source).unwrap();
        assert!(source.contains(
            "{sql_text_0, sizeof(sql_text_0), invocation->parameters, invocation->parameters_len}"
        ));
        assert!(source.contains("{sql_text_1, sizeof(sql_text_1), NULL, 0}"));
        assert!(source.contains("{sql_text_2, sizeof(sql_text_2), NULL, 0}"));
    }
}
