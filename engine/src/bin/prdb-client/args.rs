use prdb_engine::client_submit::{CancellationResult, InvocationStatus, TerminalOutcome};
use prdb_engine::supervisor::{MAX_PARAMETER_BYTES, MAX_SQL_DATA_BYTES};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) fn print_operation(label: &str, operation_id: &[u8; 16], commit_sequence: u64) {
    println!(
        "{label} (operation {}, commit {commit_sequence})",
        lower_hex(operation_id)
    );
}

pub(super) fn parse_boolean(value: &str, option: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{option} must be true or false")),
    }
}

pub(super) fn expiration_time(ttl_seconds: u64) -> Result<i64, String> {
    if ttl_seconds == 0 {
        return Err("--ttl-seconds must be positive".to_owned());
    }
    let now_us: i64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch".to_owned())?
        .as_micros()
        .try_into()
        .map_err(|_| "system time is outside the supported range".to_owned())?;
    let ttl_us = ttl_seconds
        .checked_mul(1_000_000)
        .and_then(|value| i64::try_from(value).ok())
        .ok_or_else(|| "--ttl-seconds is outside the supported range".to_owned())?;
    now_us
        .checked_add(ttl_us)
        .ok_or_else(|| "token expiration is outside the supported range".to_owned())
}

pub(super) fn read_parameters(path: Option<&PathBuf>) -> Result<Vec<u8>, String> {
    let Some(path) = path else {
        return Ok(Vec::new());
    };
    let file = File::open(path).map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    file.take(MAX_PARAMETER_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_PARAMETER_BYTES {
        bytes.fill(0);
        return Err("parameter file exceeds the 1 MiB limit".to_owned());
    }
    Ok(bytes)
}

pub(super) fn read_sql_data_file(path: Option<&PathBuf>) -> Result<Option<Vec<u8>>, String> {
    let Some(path) = path else {
        return Ok(None);
    };
    let file = File::open(path).map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    file.take(MAX_SQL_DATA_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MAX_SQL_DATA_BYTES {
        return Err("SQL data file exceeds the 1 MiB limit".to_owned());
    }
    Ok(Some(bytes))
}

pub(super) fn reject_unknown(fields: &HashMap<String, std::ffi::OsString>) -> Result<(), String> {
    if let Some(name) = fields.keys().next() {
        Err(format!("unknown option {name}"))
    } else {
        Ok(())
    }
}

pub(super) fn take_optional_text(
    fields: &mut HashMap<String, std::ffi::OsString>,
    name: &str,
) -> Result<Option<String>, String> {
    fields
        .remove(name)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| format!("{name} must be UTF-8"))
        })
        .transpose()
}

pub(super) fn take_path(
    fields: &mut HashMap<String, std::ffi::OsString>,
    name: &str,
) -> Result<PathBuf, String> {
    fields
        .remove(name)
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing required option {name}"))
}

pub(super) fn take_text(
    fields: &mut HashMap<String, std::ffi::OsString>,
    name: &str,
) -> Result<String, String> {
    fields
        .remove(name)
        .ok_or_else(|| format!("missing required option {name}"))?
        .into_string()
        .map_err(|_| format!("{name} must be UTF-8"))
}

pub(super) fn parse_number<T: std::str::FromStr>(
    value: std::ffi::OsString,
    name: &str,
) -> Result<T, String> {
    value
        .into_string()
        .map_err(|_| format!("{name} must be UTF-8"))?
        .parse()
        .map_err(|_| format!("invalid value for {name}"))
}

pub(super) fn parse_id(value: &str) -> Result<[u8; 16], String> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("--invocation-id must be 32 lowercase hexadecimal digits".to_owned());
    }
    let mut id = [0; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        id[index] = (nibble(pair[0])? << 4) | nibble(pair[1])?;
    }
    if id == [0; 16] {
        return Err("--invocation-id must be nonzero".to_owned());
    }
    Ok(id)
}

pub(super) fn nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err("invalid lowercase hexadecimal value".to_owned()),
    }
}

pub(super) fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 15) as usize] as char);
    }
    result
}

pub(super) fn status_name(status: InvocationStatus) -> &'static str {
    match status {
        InvocationStatus::Prepared => "prepared",
        InvocationStatus::Active => "active",
        InvocationStatus::Committing => "committing",
        InvocationStatus::Committed => "committed",
        InvocationStatus::Aborted => "aborted",
        InvocationStatus::Unknown => "unknown",
        InvocationStatus::Expired => "expired",
    }
}

pub(super) fn cancellation_result_name(result: CancellationResult) -> &'static str {
    match result {
        CancellationResult::Accepted => "accepted",
        CancellationResult::TooLate => "too_late",
        CancellationResult::Terminal(TerminalOutcome::Committed) => "committed",
        CancellationResult::Terminal(TerminalOutcome::Aborted) => "aborted",
        CancellationResult::Terminal(TerminalOutcome::Unknown) | CancellationResult::Unknown => {
            "unknown"
        }
    }
}
