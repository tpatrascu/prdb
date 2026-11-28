use prdb_engine::authorization::Operation;
use prdb_engine::client_credentials::read_client_credentials;
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_submit::MetadataClient;
use prdb_engine::client_submit::{ClientResultEvent, SubmissionClient, TerminalOutcome};
use prdb_engine::submission_scope::RequestedScopeBuilder;
use prdb_engine::tuple::ScalarType;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use super::*;

/// Encode a `PDB-SQL-PARAMETERS-1` JSON document into a raw PSP1 byte file.
/// This is the only way to produce a `--parameters FILE` input for `submit`:
/// unlike `interactive-sql`, `submit` never parses or encodes SQL parameters
/// itself, since a custom native program chooses its own parameter format.
/// Use this only when the submitted program calls an SDK SQL function (such
/// as `prdb_sdk_sql_run_statement_v1`) that expects PSP1-encoded parameters.
pub(super) fn parameters_encode(
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let input_path = take_path(fields, "--input")?;
    let output_path = take_path(fields, "--output")?;
    reject_unknown(fields)?;
    let input = if input_path == Path::new("-") {
        let mut bytes = Vec::new();
        io::stdin()
            .lock()
            .read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        bytes
    } else {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(&input_path)
            .map_err(|error| error.to_string())?;
        if !file
            .metadata()
            .map_err(|error| error.to_string())?
            .is_file()
        {
            return Err("--input must be a regular file".to_owned());
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| error.to_string())?;
        bytes
    };
    let encoded = prdb_engine::client_sql::encode_parameter_file(&input)
        .map_err(|error| error.to_string())?;
    write_scope_file(&output_path, &encoded)
}

pub(super) fn scope(
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

pub(super) fn discover(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let schema_name = fields
        .remove("--schema")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "--schema must be UTF-8".to_owned())
        })
        .transpose()?;
    let relation_name = fields
        .remove("--relation")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "--relation must be UTF-8".to_owned())
        })
        .transpose()?;
    reject_unknown(fields)?;
    if schema_name.is_none() && relation_name.is_some() {
        return Err("--relation requires --schema".to_owned());
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
    for schema in &schemas {
        println!("schema\t{}\t{}", lower_hex(&schema.schema_id), schema.name);
    }

    let Some(schema_name) = schema_name else {
        return Ok(());
    };
    let schema = schemas
        .iter()
        .find(|schema| schema.name == schema_name)
        .ok_or_else(|| format!("schema {schema_name:?} is not visible"))?;
    let relations = client
        .list_relations(schema.schema_id)
        .map_err(|error| error.to_string())?;
    for relation in &relations {
        println!(
            "relation\t{}\t{}\t{}",
            lower_hex(&schema.schema_id),
            lower_hex(&relation.relation_id),
            relation.name
        );
    }

    if let Some(relation_name) = relation_name {
        let relation = relations
            .iter()
            .find(|relation| relation.name == relation_name)
            .ok_or_else(|| format!("relation {relation_name:?} is not visible"))?;
        let columns = client
            .list_columns(schema.schema_id, relation.relation_id)
            .map_err(|error| error.to_string())?;
        for column in columns {
            println!(
                "column\t{}\t{}\t{}\t{}\t{}\t{}",
                lower_hex(&relation.relation_id),
                column.ordinal,
                lower_hex(&column.column_id),
                format_scalar_type(&column.scalar_type),
                if column.nullable {
                    "nullable"
                } else {
                    "not-null"
                },
                column.name
            );
        }
    }
    Ok(())
}

pub(super) fn format_scalar_type(scalar_type: &ScalarType) -> String {
    match scalar_type {
        ScalarType::Boolean => "boolean".to_owned(),
        ScalarType::SmallInt => "smallint".to_owned(),
        ScalarType::Integer => "integer".to_owned(),
        ScalarType::BigInt => "bigint".to_owned(),
        ScalarType::Decimal { precision, scale } => format!("decimal({precision},{scale})"),
        ScalarType::Text => "text".to_owned(),
        ScalarType::Varchar { max_chars } => format!("varchar({max_chars})"),
        ScalarType::Bytea => "bytea".to_owned(),
        ScalarType::Uuid => "uuid".to_owned(),
        ScalarType::Date => "date".to_owned(),
        ScalarType::TimestampTz => "timestamptz".to_owned(),
    }
}

pub(super) fn parse_operations(value: &str) -> Result<Vec<Operation>, String> {
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

pub(super) fn split_names(value: &str, option: &str) -> Result<Vec<String>, String> {
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

pub(super) fn write_scope_file(path: &Path, bytes: &[u8]) -> Result<(), String> {
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

pub(super) fn submit(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let package = take_path(fields, "--package")?;
    let receipt = take_path(fields, "--receipt")?;
    let parameters_path = fields.remove("--parameters").map(PathBuf::from);
    let sql_data_path = fields.remove("--sql-data").map(PathBuf::from);
    let deadline_ms = fields
        .remove("--deadline-ms")
        .map(|value| parse_number(value, "--deadline-ms"))
        .transpose()?
        .unwrap_or(30_000u64);
    let _ = fields.remove("--workload");
    reject_unknown(fields)?;

    let mut parameters = read_parameters(parameters_path.as_ref())?;
    let sql_data = read_sql_data_file(sql_data_path.as_ref())?;
    let credentials =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    submit_package_and_stream_with_profile(
        address,
        credentials,
        &package,
        &receipt,
        &mut parameters,
        sql_data.as_deref(),
        deadline_ms,
        false,
    )
}

#[allow(clippy::too_many_arguments)]
pub(super) fn submit_package_and_stream_with_profile(
    address: SocketAddr,
    mut credentials: ClientCredentials,
    package: &Path,
    receipt: &Path,
    parameters: &mut [u8],
    sql_data: Option<&[u8]>,
    deadline_ms: u64,
    _repository_profile: bool,
) -> Result<(), String> {
    let result = (|| {
        let mut client = SubmissionClient::connect_repository_authenticated_with_timeout(
            address,
            credentials.token_id,
            &mut credentials.token_secret,
            Duration::from_secs(10),
        )
        .map_err(|error| error.to_string())?;
        let invocation_id = match sql_data {
            Some(sql_data) => {
                client.upload_and_seal_with_sql_data(package, parameters, sql_data, deadline_ms)
            }
            None => client.upload_and_seal(package, parameters, deadline_ms),
        }
        .map_err(|error| error.to_string())?;
        eprintln!("prepared invocation {}", lower_hex(&invocation_id));
        let mut started = client
            .start(invocation_id, receipt)
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
                Some(ClientResultEvent::PackageTerminal { .. }) => {
                    return Err(
                        "unexpected interactive package terminal in v1 submission".to_owned()
                    );
                }
                None => return Err("result stream ended without a terminal outcome".to_owned()),
            }
        }
    })();
    parameters.fill(0);
    result
}
