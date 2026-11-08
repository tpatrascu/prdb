use prdb_engine::client_credentials::read_client_credentials;
use prdb_engine::client_submit::{
    ClientResultEvent, InvocationStatus, SubmissionClient, TerminalOutcome,
};
use prdb_engine::supervisor::{MAX_PARAMETER_BYTES, WorkloadClass};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;

const USAGE: &str = "Usage:\n\
  prdb-client submit --address IP:PORT --credentials FILE --package DIR --receipt FILE \\\n    [--parameters FILE] [--deadline-ms N] [--workload operational|analytical]\n\
  prdb-client status --address IP:PORT --credentials FILE --invocation-id HEX\n\n\
Submission output is provisional until a committed terminal event. START is never retried;\n\
if its reply is uncertain, use status with the invocation ID and a newly authenticated connection.";

fn main() -> std::process::ExitCode {
    match run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("prdb-client: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let mut arguments = std::env::args_os().skip(1);
    let Some(command) = arguments.next() else {
        return Err(USAGE.to_owned());
    };
    if command == "--help" || command == "-h" {
        println!("{USAGE}");
        return Ok(());
    }
    let command = command
        .into_string()
        .map_err(|_| "command must be UTF-8".to_owned())?;
    let mut fields = HashMap::<String, std::ffi::OsString>::new();
    while let Some(option) = arguments.next() {
        if option == "--help" || option == "-h" {
            println!("{USAGE}");
            return Ok(());
        }
        let option = option
            .into_string()
            .map_err(|_| "options must be UTF-8".to_owned())?;
        if !option.starts_with("--") {
            return Err(format!("unexpected argument {option}; use --help"));
        }
        let value = arguments
            .next()
            .ok_or_else(|| format!("missing value for {option}"))?;
        if fields.insert(option.clone(), value).is_some() {
            return Err(format!("duplicate option {option}"));
        }
    }

    let address = take_text(&mut fields, "--address")?
        .parse::<SocketAddr>()
        .map_err(|_| "--address must be a numeric IP:PORT socket address".to_owned())?;
    let credentials_path = take_path(&mut fields, "--credentials")?;
    match command.as_str() {
        "submit" => submit(address, credentials_path, &mut fields),
        "status" => status(address, credentials_path, &mut fields),
        _ => Err(format!("unknown command {command}\n{USAGE}")),
    }
}

fn submit(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let package = take_path(fields, "--package")?;
    let receipt = take_path(fields, "--receipt")?;
    let parameters_path = fields.remove("--parameters").map(PathBuf::from);
    let deadline_ms = fields
        .remove("--deadline-ms")
        .map(|value| parse_number(value, "--deadline-ms"))
        .transpose()?
        .unwrap_or(30_000u64);
    let workload = match fields.remove("--workload") {
        None => WorkloadClass::Operational,
        Some(value) if value == "operational" => WorkloadClass::Operational,
        Some(value) if value == "analytical" => WorkloadClass::Analytical,
        Some(_) => return Err("--workload must be operational or analytical".to_owned()),
    };
    reject_unknown(fields)?;

    let mut parameters = read_parameters(parameters_path.as_ref())?;
    let mut credentials =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = SubmissionClient::connect_authenticated(
        address,
        credentials.token_id,
        &mut credentials.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let invocation_id = client
        .upload_and_seal(&package, &parameters, deadline_ms, workload)
        .map_err(|error| error.to_string())?;
    parameters.fill(0);
    eprintln!("prepared invocation {}", lower_hex(&invocation_id));
    let mut started = client
        .start(invocation_id, &receipt)
        .map_err(|error| error.to_string())?;
    loop {
        match started.next_event().map_err(|error| error.to_string())? {
            Some(ClientResultEvent::Data(data)) => {
                io::stdout()
                    .write_all(&data)
                    .and_then(|()| io::stdout().flush())
                    .map_err(|error| error.to_string())?;
            }
            Some(ClientResultEvent::Terminal(terminal)) => {
                eprintln!(
                    "invocation {} outcome={:?} reason={}",
                    lower_hex(&terminal.invocation_id),
                    terminal.outcome,
                    terminal.reason_code
                );
                return match terminal.outcome {
                    TerminalOutcome::Committed => Ok(()),
                    TerminalOutcome::Aborted | TerminalOutcome::Unknown => {
                        Err("invocation did not commit".to_owned())
                    }
                };
            }
            None => return Err("result stream ended without a terminal outcome".to_owned()),
        }
    }
}

fn status(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let invocation_id = parse_id(&take_text(fields, "--invocation-id")?)?;
    reject_unknown(fields)?;
    let mut credentials =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = SubmissionClient::connect_authenticated(
        address,
        credentials.token_id,
        &mut credentials.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let status = client
        .status(invocation_id)
        .map_err(|error| error.to_string())?;
    println!("{}", status_name(status));
    Ok(())
}

fn read_parameters(path: Option<&PathBuf>) -> Result<Vec<u8>, String> {
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

fn reject_unknown(fields: &HashMap<String, std::ffi::OsString>) -> Result<(), String> {
    if let Some(name) = fields.keys().next() {
        Err(format!("unknown option {name}"))
    } else {
        Ok(())
    }
}

fn take_path(
    fields: &mut HashMap<String, std::ffi::OsString>,
    name: &str,
) -> Result<PathBuf, String> {
    fields
        .remove(name)
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing required option {name}"))
}

fn take_text(
    fields: &mut HashMap<String, std::ffi::OsString>,
    name: &str,
) -> Result<String, String> {
    fields
        .remove(name)
        .ok_or_else(|| format!("missing required option {name}"))?
        .into_string()
        .map_err(|_| format!("{name} must be UTF-8"))
}

fn parse_number<T: std::str::FromStr>(value: std::ffi::OsString, name: &str) -> Result<T, String> {
    value
        .into_string()
        .map_err(|_| format!("{name} must be UTF-8"))?
        .parse()
        .map_err(|_| format!("invalid value for {name}"))
}

fn parse_id(value: &str) -> Result<[u8; 16], String> {
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

fn nibble(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err("invalid lowercase hexadecimal value".to_owned()),
    }
}

fn lower_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 15) as usize] as char);
    }
    result
}

fn status_name(status: InvocationStatus) -> &'static str {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invocation_id_parser_requires_canonical_nonzero_hex() {
        assert_eq!(parse_id("0102030405060708090a0b0c0d0e0f10").unwrap()[0], 1);
        assert!(parse_id("0102030405060708090a0b0c0d0e0f1A").is_err());
        assert!(parse_id("00000000000000000000000000000000").is_err());
        assert!(parse_id("1").is_err());
    }
}
