use prdb_engine::authorization::Operation;
use prdb_engine::client_credentials::{read_client_credentials, write_client_credentials};
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_submit::MetadataClient;
use prdb_engine::client_submit::{
    AdminClient, ClientResultEvent, InvocationStatus, SubmissionClient, TerminalOutcome,
};
use prdb_engine::submission_scope::RequestedScopeBuilder;
use prdb_engine::supervisor::{MAX_PARAMETER_BYTES, WorkloadClass};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

const USAGE: &str = "Usage:\n\
  prdb-client submit --address IP:PORT --credentials FILE --package DIR --receipt FILE \\\n    [--parameters FILE] [--deadline-ms N] [--workload operational|analytical]\n\
  prdb-client status --address IP:PORT --credentials FILE --invocation-id HEX\n\n\
  prdb-client scope --address IP:PORT --credentials FILE --schema NAME --output FILE \\\n    [--relation NAME --operations select,insert,update,delete [--columns NAME,...]]\n\n\
Submission output is provisional until a committed terminal event. START is never retried;\n\
if its reply is uncertain, use status with the invocation ID and a newly authenticated connection.\n\
Scope files contain only permissions visible to the authenticated identity and remain a request ceiling.\n\
Token secrets are saved only to mode-0600 credential files and are never printed.\n\
  prdb-client token-issue --address IP:PORT --credentials ADMIN_FILE --user-id HEX\n\
    --label TEXT --ttl-seconds N --output-credentials FILE\n\
  prdb-client token-revoke --address IP:PORT --credentials ADMIN_FILE --token-id HEX\n\n\
Token secrets are saved only to mode-0600 credential files and are never printed.\n\
For rotation, save a replacement credential, verify it, then revoke the old token.";

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
        "scope" => scope(address, credentials_path, &mut fields),
        "token-issue" => token_issue(address, credentials_path, &mut fields),
        "token-revoke" => token_revoke(address, credentials_path, &mut fields),
        _ => Err(format!("unknown command {command}\n{USAGE}")),
    }
}

fn scope(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let schema_name = take_text(fields, "--schema")?;
    let output_path = take_path(fields, "--output")?;
    let relation_name = fields
        .remove("--relation")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "--relation must be UTF-8".to_owned())
        })
        .transpose()?;
    let operations = fields
        .remove("--operations")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "--operations must be UTF-8".to_owned())
                .and_then(|value| parse_operations(&value))
        })
        .transpose()?;
    let column_names = fields
        .remove("--columns")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "--columns must be UTF-8".to_owned())
                .and_then(|value| split_names(&value, "--columns"))
        })
        .transpose()?;
    reject_unknown(fields)?;
    if relation_name.is_none() && (operations.is_some() || column_names.is_some()) {
        return Err("--operations and --columns require --relation".to_owned());
    }
    if relation_name.is_some() && operations.is_none() {
        return Err("--relation requires --operations".to_owned());
    }
    if column_names.is_some() && relation_name.is_none() {
        return Err("--columns requires --relation".to_owned());
    }

    let mut credentials =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = MetadataClient::connect_authenticated(
        address,
        credentials.token_id,
        &mut credentials.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let schemas = client.list_schemas().map_err(|error| error.to_string())?;
    let schema = schemas
        .iter()
        .find(|schema| schema.name == schema_name)
        .ok_or_else(|| format!("schema {schema_name:?} is not visible"))?;
    let mut builder = RequestedScopeBuilder::new();
    match relation_name {
        None => builder
            .include_schema(schema)
            .map_err(|error| format!("invalid requested scope: {error:?}"))?,
        Some(relation_name) => {
            let relations = client
                .list_relations(schema.schema_id)
                .map_err(|error| error.to_string())?;
            let relation = relations
                .iter()
                .find(|relation| relation.name == relation_name)
                .ok_or_else(|| format!("relation {relation_name:?} is not visible"))?;
            let operations = operations.expect("validated relation operation list");
            match column_names {
                None => builder
                    .include_relation(schema, relation, &operations)
                    .map_err(|error| format!("invalid requested scope: {error:?}"))?,
                Some(column_names) => {
                    let columns = client
                        .list_columns(schema.schema_id, relation.relation_id)
                        .map_err(|error| error.to_string())?;
                    for name in column_names {
                        let column = columns
                            .iter()
                            .find(|column| column.name == name)
                            .ok_or_else(|| format!("column {name:?} is not visible"))?;
                        builder
                            .include_column(schema, relation, column, &operations)
                            .map_err(|error| format!("invalid requested scope: {error:?}"))?;
                    }
                }
            }
        }
    }
    let bytes = builder
        .encode()
        .map_err(|error| format!("invalid requested scope: {error:?}"))?;
    write_scope_file(&output_path, &bytes)?;
    let count = u16::from_be_bytes([bytes[4], bytes[5]]);
    println!("wrote {} capabilities to {}", count, output_path.display());
    Ok(())
}

fn parse_operations(value: &str) -> Result<Vec<Operation>, String> {
    let mut operations = Vec::new();
    for name in split_names(value, "--operations")? {
        let operation = match name.as_str() {
            "select" => Operation::Select,
            "insert" => Operation::Insert,
            "update" => Operation::Update,
            "delete" => Operation::Delete,
            _ => return Err(format!("unsupported operation {name:?}")),
        };
        if operations.contains(&operation) {
            return Err(format!("duplicate operation {name:?}"));
        }
        operations.push(operation);
    }
    Ok(operations)
}

fn split_names(value: &str, option: &str) -> Result<Vec<String>, String> {
    let names = value
        .split(',')
        .map(str::trim)
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if names.is_empty() || names.iter().any(String::is_empty) {
        return Err(format!("{option} must be a comma-separated nonempty list"));
    }
    let mut unique = names.clone();
    unique.sort();
    unique.dedup();
    if unique.len() != names.len() {
        return Err(format!("{option} contains a duplicate value"));
    }
    Ok(names)
}

fn write_scope_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = fs::canonicalize(parent).map_err(|error| error.to_string())?;
    let name = path
        .file_name()
        .ok_or_else(|| "invalid --output path".to_owned())?;
    let target = parent.join(name);
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).map_err(|error| error.to_string())?;
    let temporary = parent.join(format!(".psc1-{}.tmp", lower_hex(&random)));
    random.fill(0);
    let result = (|| {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::hard_link(&temporary, &target)?;
        fs::remove_file(&temporary)?;
        File::open(parent)?.sync_all()?;
        Ok::<(), io::Error>(())
    })();
    let _ = fs::remove_file(&temporary);
    result.map_err(|error| error.to_string())
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

fn token_issue(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let user_id = parse_id(&take_text(fields, "--user-id")?)?;
    let label = take_text(fields, "--label")?;
    let ttl_seconds: u64 = parse_number(
        fields
            .remove("--ttl-seconds")
            .ok_or_else(|| "missing required option --ttl-seconds".to_owned())?,
        "--ttl-seconds",
    )?;
    let output_path = take_path(fields, "--output-credentials")?;
    reject_unknown(fields)?;
    let expires_at_us = expiration_time(ttl_seconds)?;

    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let issued = client
        .issue_token(user_id, &label, expires_at_us)
        .map_err(|error| error.to_string())?;
    let token_id = issued.token_id;
    let operation_id = issued.operation_id;
    let new_credentials = ClientCredentials {
        token_id,
        token_secret: *issued.token_secret(),
    };
    write_client_credentials(&output_path, &new_credentials).map_err(|error| {
        format!(
            "token issuance committed (operation {}, token {}) but saving {} failed: {error}; revoke token {} before retrying",
            lower_hex(&operation_id),
            lower_hex(&token_id),
            output_path.display(),
            lower_hex(&token_id)
        )
    })?;
    println!(
        "saved token {} to {} (operation {}, expires at {expires_at_us} us)",
        lower_hex(&token_id),
        output_path.display(),
        lower_hex(&operation_id)
    );
    Ok(())
}

fn token_revoke(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let token_id = parse_id(&take_text(fields, "--token-id")?)?;
    reject_unknown(fields)?;
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let result = client
        .revoke_token(token_id)
        .map_err(|error| error.to_string())?;
    println!(
        "revoked token {} (operation {}, commit {})",
        lower_hex(&token_id),
        lower_hex(&result.operation_id),
        result.commit_sequence
    );
    Ok(())
}

fn expiration_time(ttl_seconds: u64) -> Result<i64, String> {
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
    use std::os::unix::fs::PermissionsExt;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn invocation_id_parser_requires_canonical_nonzero_hex() {
        assert_eq!(parse_id("0102030405060708090a0b0c0d0e0f10").unwrap()[0], 1);
        assert!(parse_id("0102030405060708090a0b0c0d0e0f1A").is_err());
        assert!(parse_id("00000000000000000000000000000000").is_err());
        assert!(parse_id("1").is_err());
    }

    #[test]
    fn scope_operation_and_name_lists_are_strict() {
        assert_eq!(
            parse_operations("select,insert,delete").unwrap(),
            vec![Operation::Select, Operation::Insert, Operation::Delete]
        );
        assert!(parse_operations("select,select").is_err());
        assert!(parse_operations("usage").is_err());
        assert!(split_names("id,name", "--columns").is_ok());
        assert!(split_names("id,id", "--columns").is_err());
        assert!(split_names("id,,name", "--columns").is_err());
    }

    #[test]
    fn scope_file_is_private_and_never_overwritten() {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory =
            std::env::temp_dir().join(format!("prdb-client-scope-{}-{nonce}", std::process::id()));
        fs::create_dir(&directory).unwrap();
        let path = directory.join("requested.psc1");
        write_scope_file(&path, b"PSC1test").unwrap();
        assert_eq!(fs::read(&path).unwrap(), b"PSC1test");
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(write_scope_file(&path, b"replacement").is_err());
        assert_eq!(fs::read(&path).unwrap(), b"PSC1test");
        fs::remove_file(path).unwrap();
        fs::remove_dir(directory).unwrap();
    }

    #[test]
    fn token_expiration_requires_positive_bounded_lifetime() {
        assert!(expiration_time(0).is_err());
        let expiry = expiration_time(60).unwrap();
        let now: i64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
            .try_into()
            .unwrap();
        assert!(expiry > now);
        assert!(expiration_time(u64::MAX).is_err());
    }
}
