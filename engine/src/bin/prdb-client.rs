use prdb_engine::authorization::Operation;
use prdb_engine::client_credentials::{read_client_credentials, write_client_credentials};
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_submit::MetadataClient;
use prdb_engine::client_submit::{
    AdminClient, AdminColumnDefault, AdminColumnDefinition, AdminCreatePolicyRequest,
    AdminObjectGrantTarget, AdminObjectPrivilege, AdminPolicyEffect, AdminPolicyKind,
    AdminPolicyOperation, AdminPolicyRecipient, AdminSystemPrivilege, CancellationResult,
    ClientResultEvent, InvocationStatus, SubmissionClient, TerminalOutcome,
};
use prdb_engine::submission_scope::RequestedScopeBuilder;
use prdb_engine::supervisor::{MAX_PARAMETER_BYTES, WorkloadClass};
use prdb_engine::tuple::{ScalarType, ScalarValue};
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
  prdb-client cancel --address IP:PORT --credentials FILE --invocation-id HEX\n\n\
  prdb-client admin-cancel --address IP:PORT --credentials ADMIN_FILE --invocation-id HEX\n\n\
  prdb-client scope --address IP:PORT --credentials FILE --schema NAME --output FILE \\\n    [--relation NAME --operations select,insert,update,delete [--columns NAME,...]]\n\n\
Submission output is provisional until a committed terminal event. START is never retried;\n\
if its reply is uncertain, use status with the invocation ID and a newly authenticated connection.\n\
Scope files contain only permissions visible to the authenticated identity and remain a request ceiling.\n\
Token secrets are saved only to mode-0600 credential files and are never printed.\n\
  prdb-client token-issue --address IP:PORT --credentials ADMIN_FILE --user-id HEX\n\
    --label TEXT --ttl-seconds N --output-credentials FILE\n\
  prdb-client token-revoke --address IP:PORT --credentials ADMIN_FILE --token-id HEX\n\n\
  prdb-client user-create|role-create --address IP:PORT --credentials ADMIN_FILE --name NAME\n\
  prdb-client user-rename|role-rename --address IP:PORT --credentials ADMIN_FILE --id HEX --name NAME\n\
  prdb-client user-drop|role-drop --address IP:PORT --credentials ADMIN_FILE --id HEX\n\
  prdb-client user-login --address IP:PORT --credentials ADMIN_FILE --user-id HEX --enabled true|false\n\
  prdb-client role-grant|role-revoke --address IP:PORT --credentials ADMIN_FILE --member-id HEX --role-id HEX\n\n\
  prdb-client schema-create --address IP:PORT --credentials ADMIN_FILE --name NAME --owner-id HEX\n\
  prdb-client schema-rename --address IP:PORT --credentials ADMIN_FILE --id HEX --name NAME\n\
  prdb-client schema-drop --address IP:PORT --credentials ADMIN_FILE --id HEX\n\
  prdb-client table-create --address IP:PORT --credentials ADMIN_FILE --schema-id HEX --name NAME \\\n    --columns 'name:type:nullable[;name:type:nullable...]'\n\
  prdb-client table-rename --address IP:PORT --credentials ADMIN_FILE --id HEX --name NAME\n\
  prdb-client table-drop --address IP:PORT --credentials ADMIN_FILE --id HEX\n\n\
  prdb-client schema-owner-set --address IP:PORT --credentials ADMIN_FILE --schema-id HEX --owner-id HEX\n\
  prdb-client table-owner-set --address IP:PORT --credentials ADMIN_FILE --table-id HEX --owner-id HEX\n\n\
  prdb-client column-add --address IP:PORT --credentials ADMIN_FILE --table-id HEX --column name:type:nullable\n\
  prdb-client column-rename --address IP:PORT --credentials ADMIN_FILE --table-id HEX --column-id HEX --name NAME\n\
  prdb-client column-drop --address IP:PORT --credentials ADMIN_FILE --table-id HEX --column-id HEX\n\n\
  prdb-client column-nullability --address IP:PORT --credentials ADMIN_FILE --table-id HEX --column-id HEX --nullable true|false\n\
  prdb-client column-default-set --address IP:PORT --credentials ADMIN_FILE --table-id HEX --column-id HEX --type TYPE --value VALUE\n\
  prdb-client column-default-drop --address IP:PORT --credentials ADMIN_FILE --table-id HEX --column-id HEX\n\n\
  prdb-client index-create --address IP:PORT --credentials ADMIN_FILE --table-id HEX --name NAME --columns HEX,... [--unique true|false]\n\
  prdb-client index-rebuild --address IP:PORT --credentials ADMIN_FILE --index-id HEX --columns HEX,... [--unique true|false]\n\
  prdb-client index-drop --address IP:PORT --credentials ADMIN_FILE --index-id HEX\n\n\
  prdb-client unique-create|primary-key-create --address IP:PORT --credentials ADMIN_FILE \\
    --table-id HEX --columns HEX,...\n\
  prdb-client unique-drop|primary-key-drop --address IP:PORT --credentials ADMIN_FILE --constraint-id HEX\n\n\
  prdb-client foreign-key-create --address IP:PORT --credentials ADMIN_FILE \\
    --source-table-id HEX --source-columns HEX,... --target-constraint-id HEX\n\
  prdb-client foreign-key-drop --address IP:PORT --credentials ADMIN_FILE --foreign-key-id HEX\n\n\
  prdb-client check-create --address IP:PORT --credentials ADMIN_FILE \\
    --table-id HEX --expression-eir1 HEX --columns HEX,... (empty for constants)\n\
  prdb-client check-drop --address IP:PORT --credentials ADMIN_FILE --constraint-id HEX\n\n\
  prdb-client object-grant|object-revoke --address IP:PORT --credentials ADMIN_FILE \\
    --grantee-id HEX --object-id HEX --privilege NAME --target-scope object|column --target-id HEX\n\n\
  prdb-client policy-create --address IP:PORT --credentials ADMIN_FILE --table-id HEX --name NAME \\
    --kind row|cell --operation select|insert|update|delete --effect allow|restrict \\
    --recipients user:HEX,role:HEX [--target-columns HEX,...] \\
    [--using-expression-eir1 HEX] [--with-check-expression-eir1 HEX]\n\
  prdb-client policy-drop --address IP:PORT --credentials ADMIN_FILE --policy-id HEX\n\n\
  prdb-client system-privilege-grant|system-privilege-revoke --address IP:PORT \\
    --credentials ADMIN_FILE --grantee-id HEX --privilege manage_users|manage_roles|create_schema\n\n\
Column types: boolean, smallint, integer, bigint, decimal(P,S), text, varchar(N), bytea, uuid, date, timestamptz.\n\
Column nullability is nullable or not-null; not-null columns require the UUID identity modifier.\n\n\
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
        "cancel" => cancel(address, credentials_path, &mut fields),
        "admin-cancel" => admin_cancel(address, credentials_path, &mut fields),
        "scope" => scope(address, credentials_path, &mut fields),
        "token-issue" => token_issue(address, credentials_path, &mut fields),
        "token-revoke" => token_revoke(address, credentials_path, &mut fields),
        "schema-create" | "schema-rename" | "schema-drop" | "schema-owner-set" | "table-create"
        | "table-rename" | "table-drop" | "table-owner-set" => {
            schema_table_admin(&command, address, credentials_path, &mut fields)
        }
        "column-add"
        | "column-rename"
        | "column-drop"
        | "column-nullability"
        | "column-default-set"
        | "column-default-drop" => column_admin(&command, address, credentials_path, &mut fields),
        "index-create" | "index-rebuild" | "index-drop" => {
            index_admin(&command, address, credentials_path, &mut fields)
        }
        "unique-create" | "primary-key-create" | "unique-drop" | "primary-key-drop" => {
            key_constraint_admin(&command, address, credentials_path, &mut fields)
        }
        "system-privilege-grant" | "system-privilege-revoke" => {
            system_privilege_admin(&command, address, credentials_path, &mut fields)
        }
        "foreign-key-create" | "foreign-key-drop" => {
            foreign_key_admin(&command, address, credentials_path, &mut fields)
        }
        "check-create" | "check-drop" => {
            check_constraint_admin(&command, address, credentials_path, &mut fields)
        }
        "object-grant" | "object-revoke" => {
            object_grant_admin(&command, address, credentials_path, &mut fields)
        }
        "policy-create" | "policy-drop" => {
            policy_admin(&command, address, credentials_path, &mut fields)
        }
        "user-create" | "user-rename" | "user-drop" | "user-login" | "role-create"
        | "role-rename" | "role-drop" | "role-grant" | "role-revoke" => {
            identity_admin(&command, address, credentials_path, &mut fields)
        }
        _ => Err(format!("unknown command {command}\n{USAGE}")),
    }
}

fn schema_table_admin(
    command: &str,
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    match command {
        "schema-create" => {
            let name = take_text(fields, "--name")?;
            let owner_id = parse_id(&take_text(fields, "--owner-id")?)?;
            reject_unknown(fields)?;
            let result = client
                .create_schema(&name, owner_id)
                .map_err(|error| error.to_string())?;
            println!(
                "created schema {} (operation {}, commit {})",
                lower_hex(&result.identity_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
        "schema-rename" | "table-rename" => {
            let id = parse_id(&take_text(fields, "--id")?)?;
            let name = take_text(fields, "--name")?;
            reject_unknown(fields)?;
            let result = if command == "schema-rename" {
                client.rename_schema(id, &name)
            } else {
                client.rename_table(id, &name)
            }
            .map_err(|error| error.to_string())?;
            print_operation(
                "renamed object",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        "schema-drop" | "table-drop" => {
            let id = parse_id(&take_text(fields, "--id")?)?;
            reject_unknown(fields)?;
            let result = if command == "schema-drop" {
                client.drop_schema(id)
            } else {
                client.drop_table(id)
            }
            .map_err(|error| error.to_string())?;
            print_operation(
                "dropped object",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        "schema-owner-set" => {
            let (schema_id, owner_id) = take_schema_owner_args(fields)?;
            let result = client
                .set_schema_owner(schema_id, owner_id)
                .map_err(|error| error.to_string())?;
            print_operation(
                "changed schema owner",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        "table-create" => {
            let schema_id = parse_id(&take_text(fields, "--schema-id")?)?;
            let name = take_text(fields, "--name")?;
            let columns = parse_column_definitions(&take_text(fields, "--columns")?)?;
            reject_unknown(fields)?;
            let result = client
                .create_table(schema_id, &name, &columns)
                .map_err(|error| error.to_string())?;
            println!(
                "created table {} with {} columns (operation {}, commit {})",
                lower_hex(&result.table_id),
                result.column_ids.len(),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
        "table-owner-set" => {
            let (table_id, owner_id) = take_table_owner_args(fields)?;
            let result = client
                .set_table_owner(table_id, owner_id)
                .map_err(|error| error.to_string())?;
            print_operation(
                "changed table owner",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        _ => return Err(format!("unsupported schema/table command {command}")),
    }
    Ok(())
}

fn take_table_owner_args(
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<([u8; 16], [u8; 16]), String> {
    let table_id = parse_id(&take_text(fields, "--table-id")?)?;
    let owner_id = parse_id(&take_text(fields, "--owner-id")?)?;
    reject_unknown(fields)?;
    Ok((table_id, owner_id))
}

fn take_schema_owner_args(
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<([u8; 16], [u8; 16]), String> {
    let schema_id = parse_id(&take_text(fields, "--schema-id")?)?;
    let owner_id = parse_id(&take_text(fields, "--owner-id")?)?;
    reject_unknown(fields)?;
    Ok((schema_id, owner_id))
}

fn column_admin(
    command: &str,
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let table_id = parse_id(&take_text(fields, "--table-id")?)?;
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    match command {
        "column-add" => {
            let column_text = take_text(fields, "--column")?;
            let column = parse_added_column(&column_text)?;
            reject_unknown(fields)?;
            let result = client
                .add_column(table_id, &column)
                .map_err(|error| error.to_string())?;
            println!(
                "added column {} to table {} (operation {}, commit {})",
                lower_hex(&result.column_id),
                lower_hex(&result.table_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
        "column-rename" => {
            let column_id = parse_id(&take_text(fields, "--column-id")?)?;
            let name = take_text(fields, "--name")?;
            reject_unknown(fields)?;
            let result = client
                .rename_column(table_id, column_id, &name)
                .map_err(|error| error.to_string())?;
            print_operation(
                "renamed column",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        "column-drop" => {
            let column_id = parse_id(&take_text(fields, "--column-id")?)?;
            reject_unknown(fields)?;
            let result = client
                .drop_column(table_id, column_id)
                .map_err(|error| error.to_string())?;
            print_operation(
                "dropped column",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        "column-nullability" => {
            let column_id = parse_id(&take_text(fields, "--column-id")?)?;
            let nullable = parse_boolean(&take_text(fields, "--nullable")?, "--nullable")?;
            reject_unknown(fields)?;
            let result = client
                .alter_column_nullability(table_id, column_id, nullable)
                .map_err(|error| error.to_string())?;
            print_operation(
                "changed column nullability",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        "column-default-set" => {
            let column_id = parse_id(&take_text(fields, "--column-id")?)?;
            let scalar_type = parse_scalar_type(&take_text(fields, "--type")?)?;
            let value = take_text(fields, "--value")?;
            let default = parse_column_default(&value, scalar_type)?;
            reject_unknown(fields)?;
            let result = client
                .set_column_default(table_id, column_id, scalar_type, default)
                .map_err(|error| error.to_string())?;
            print_operation(
                "set column default",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        "column-default-drop" => {
            let column_id = parse_id(&take_text(fields, "--column-id")?)?;
            reject_unknown(fields)?;
            let result = client
                .drop_column_default(table_id, column_id)
                .map_err(|error| error.to_string())?;
            print_operation(
                "dropped column default",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        _ => return Err(format!("unsupported column command {command}")),
    }
    Ok(())
}

fn index_admin(
    command: &str,
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    match command {
        "index-create" => {
            let table_id = parse_id(&take_text(fields, "--table-id")?)?;
            let name = take_text(fields, "--name")?;
            let column_ids = parse_id_list(&take_text(fields, "--columns")?, "--columns")?;
            let unique = take_optional_boolean(fields, "--unique")?.unwrap_or(false);
            reject_unknown(fields)?;
            let result = client
                .create_index(table_id, &name, &column_ids, unique)
                .map_err(|error| error.to_string())?;
            println!(
                "created index {} (operation {}, commit {})",
                lower_hex(&result.index_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
        "index-rebuild" => {
            let index_id = parse_id(&take_text(fields, "--index-id")?)?;
            let column_ids = parse_id_list(&take_text(fields, "--columns")?, "--columns")?;
            let unique = take_optional_boolean(fields, "--unique")?.unwrap_or(false);
            reject_unknown(fields)?;
            let result = client
                .rebuild_index(index_id, &column_ids, unique)
                .map_err(|error| error.to_string())?;
            print_operation(
                "rebuilt index",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        "index-drop" => {
            let index_id = parse_id(&take_text(fields, "--index-id")?)?;
            reject_unknown(fields)?;
            let result = client
                .drop_index(index_id)
                .map_err(|error| error.to_string())?;
            print_operation(
                "dropped index",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        _ => return Err(format!("unsupported index command {command}")),
    }
    Ok(())
}

fn key_constraint_admin(
    command: &str,
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    match command {
        "unique-create" | "primary-key-create" => {
            let table_id = parse_id(&take_text(fields, "--table-id")?)?;
            let column_ids = parse_constraint_columns(&take_text(fields, "--columns")?)?;
            reject_unknown(fields)?;
            let result = if command == "unique-create" {
                client.create_unique_constraint(table_id, &column_ids)
            } else {
                client.create_primary_key(table_id, &column_ids)
            }
            .map_err(|error| error.to_string())?;
            println!(
                "created {} constraint {} with backing index {} (operation {}, commit {})",
                if command == "unique-create" {
                    "unique"
                } else {
                    "primary-key"
                },
                lower_hex(&result.constraint_id),
                lower_hex(&result.index_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
        "unique-drop" | "primary-key-drop" => {
            let constraint_id = parse_id(&take_text(fields, "--constraint-id")?)?;
            reject_unknown(fields)?;
            let result = if command == "unique-drop" {
                client.drop_unique_constraint(constraint_id)
            } else {
                client.drop_primary_key(constraint_id)
            }
            .map_err(|error| error.to_string())?;
            print_operation(
                "dropped key constraint",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        _ => return Err(format!("unsupported key constraint command {command}")),
    }
    Ok(())
}

fn parse_constraint_columns(value: &str) -> Result<Vec<[u8; 16]>, String> {
    let column_ids = parse_id_list(value, "--columns")?;
    if column_ids.is_empty() || column_ids.len() > prdb_engine::tuple::MAX_COLUMNS {
        return Err(format!(
            "--columns must contain between 1 and {} IDs",
            prdb_engine::tuple::MAX_COLUMNS
        ));
    }
    Ok(column_ids)
}

fn parse_optional_constraint_columns(value: &str) -> Result<Vec<[u8; 16]>, String> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    parse_constraint_columns(value)
}

fn parse_lowercase_hex_bytes(value: &str) -> Result<Vec<u8>, String> {
    if value.is_empty()
        || !value.len().is_multiple_of(2)
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err("hex input must be nonempty, even-length lowercase hexadecimal".to_owned());
    }
    (0..value.len())
        .step_by(2)
        .map(|offset| {
            u8::from_str_radix(&value[offset..offset + 2], 16)
                .map_err(|_| "hex input is malformed".to_owned())
        })
        .collect()
}

fn parse_system_privilege(value: &str) -> Result<AdminSystemPrivilege, String> {
    match value {
        "manage_users" => Ok(AdminSystemPrivilege::ManageUsers),
        "manage_roles" => Ok(AdminSystemPrivilege::ManageRoles),
        "create_schema" => Ok(AdminSystemPrivilege::CreateSchema),
        _ => Err("--privilege must be manage_users, manage_roles, or create_schema".to_owned()),
    }
}

fn system_privilege_admin(
    command: &str,
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let grantee_id = parse_id(&take_text(fields, "--grantee-id")?)?;
    let privilege = parse_system_privilege(&take_text(fields, "--privilege")?)?;
    reject_unknown(fields)?;
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let result = if command == "system-privilege-grant" {
        client.grant_system_privilege(grantee_id, privilege)
    } else {
        client.revoke_system_privilege(grantee_id, privilege)
    }
    .map_err(|error| error.to_string())?;
    print_operation(
        if command == "system-privilege-grant" {
            "granted system privilege"
        } else {
            "revoked system privilege"
        },
        &result.operation_id,
        result.commit_sequence,
    );
    Ok(())
}

fn parse_object_privilege(value: &str) -> Result<AdminObjectPrivilege, String> {
    match value {
        "usage" => Ok(AdminObjectPrivilege::Usage),
        "select" => Ok(AdminObjectPrivilege::Select),
        "insert" => Ok(AdminObjectPrivilege::Insert),
        "update" => Ok(AdminObjectPrivilege::Update),
        "delete" => Ok(AdminObjectPrivilege::Delete),
        "references" => Ok(AdminObjectPrivilege::References),
        "create" => Ok(AdminObjectPrivilege::Create),
        "alter" => Ok(AdminObjectPrivilege::Alter),
        "index" => Ok(AdminObjectPrivilege::Index),
        "manage_policies" => Ok(AdminObjectPrivilege::ManagePolicies),
        "manage_grants" => Ok(AdminObjectPrivilege::ManageGrants),
        _ => Err("--privilege is not a supported object privilege".to_owned()),
    }
}

fn object_grant_admin(
    command: &str,
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let grantee_id = parse_id(&take_text(fields, "--grantee-id")?)?;
    let object_id = parse_id(&take_text(fields, "--object-id")?)?;
    let privilege = parse_object_privilege(&take_text(fields, "--privilege")?)?;
    let target_scope = take_text(fields, "--target-scope")?;
    let target_id = parse_id(&take_text(fields, "--target-id")?)?;
    let target = match target_scope.as_str() {
        "object" if target_id == object_id => AdminObjectGrantTarget::Object,
        "column" => AdminObjectGrantTarget::Column(target_id),
        "object" => return Err("--target-id must equal --object-id for object scope".to_owned()),
        _ => return Err("--target-scope must be object or column".to_owned()),
    };
    reject_unknown(fields)?;
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let result = if command == "object-grant" {
        client.grant_object_privilege(grantee_id, object_id, privilege, target)
    } else {
        client.revoke_object_privilege(grantee_id, object_id, privilege, target)
    }
    .map_err(|error| error.to_string())?;
    print_operation(
        if command == "object-grant" {
            "granted object privilege"
        } else {
            "revoked object privilege"
        },
        &result.operation_id,
        result.commit_sequence,
    );
    Ok(())
}

fn policy_admin(
    command: &str,
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    match command {
        "policy-create" => {
            let table_id = parse_id(&take_text(fields, "--table-id")?)?;
            let name = take_text(fields, "--name")?;
            let kind = match take_text(fields, "--kind")?.as_str() {
                "row" => AdminPolicyKind::Row,
                "cell" => AdminPolicyKind::Cell,
                _ => return Err("--kind must be row or cell".to_owned()),
            };
            let operation = match take_text(fields, "--operation")?.as_str() {
                "select" => AdminPolicyOperation::Select,
                "insert" => AdminPolicyOperation::Insert,
                "update" => AdminPolicyOperation::Update,
                "delete" => AdminPolicyOperation::Delete,
                _ => return Err("--operation must be select, insert, update, or delete".to_owned()),
            };
            let effect = match take_text(fields, "--effect")?.as_str() {
                "allow" => AdminPolicyEffect::Allow,
                "restrict" => AdminPolicyEffect::Restrict,
                _ => return Err("--effect must be allow or restrict".to_owned()),
            };
            let recipients = parse_policy_recipients(&take_text(fields, "--recipients")?)?;
            let target_columns = fields
                .remove("--target-columns")
                .map(|value| {
                    value
                        .into_string()
                        .map_err(|_| "--target-columns must be UTF-8".to_owned())
                        .and_then(|value| parse_optional_constraint_columns(&value))
                })
                .transpose()?
                .unwrap_or_default();
            let using_expression =
                take_optional_policy_expression(fields, "--using-expression-eir1")?;
            let with_check_expression =
                take_optional_policy_expression(fields, "--with-check-expression-eir1")?;
            reject_unknown(fields)?;
            let result = client
                .create_policy(AdminCreatePolicyRequest {
                    table_id,
                    name: &name,
                    kind,
                    operation,
                    effect,
                    recipients: &recipients,
                    target_column_ids: &target_columns,
                    using_expression: using_expression.as_ref(),
                    with_check_expression: with_check_expression.as_ref(),
                })
                .map_err(|error| error.to_string())?;
            println!(
                "created policy {} (operation {}, commit {})",
                lower_hex(&result.policy_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
        "policy-drop" => {
            let policy_id = parse_id(&take_text(fields, "--policy-id")?)?;
            reject_unknown(fields)?;
            let result = client
                .drop_policy(policy_id)
                .map_err(|error| error.to_string())?;
            print_operation(
                "dropped policy",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        _ => return Err(format!("unsupported policy command {command}")),
    }
    Ok(())
}

fn parse_policy_recipients(value: &str) -> Result<Vec<AdminPolicyRecipient>, String> {
    if value.is_empty() {
        return Err("--recipients must contain user:HEX or role:HEX values".to_owned());
    }
    value
        .split(',')
        .map(|entry| {
            let (kind, id) = entry
                .split_once(':')
                .ok_or_else(|| "--recipients entries must use user:HEX or role:HEX".to_owned())?;
            let id = parse_id(id)?;
            match kind {
                "user" => Ok(AdminPolicyRecipient::User(id)),
                "role" => Ok(AdminPolicyRecipient::Role(id)),
                _ => Err("--recipients entries must use user:HEX or role:HEX".to_owned()),
            }
        })
        .collect()
}

fn take_optional_policy_expression(
    fields: &mut HashMap<String, std::ffi::OsString>,
    option: &str,
) -> Result<Option<prdb_engine::authorization::Expr>, String> {
    fields
        .remove(option)
        .map(|value| {
            let value = value
                .into_string()
                .map_err(|_| format!("{option} must be UTF-8"))?;
            let bytes = parse_lowercase_hex_bytes(&value)?;
            prdb_engine::expression::decode_expression(&bytes)
                .map_err(|_| format!("{option} must contain a valid EIR1 expression"))
        })
        .transpose()
}

fn foreign_key_admin(
    command: &str,
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    match command {
        "foreign-key-create" => {
            let source_table_id = parse_id(&take_text(fields, "--source-table-id")?)?;
            let source_column_ids =
                parse_constraint_columns(&take_text(fields, "--source-columns")?)?;
            let target_constraint_id = parse_id(&take_text(fields, "--target-constraint-id")?)?;
            reject_unknown(fields)?;
            let result = client
                .create_foreign_key(source_table_id, &source_column_ids, target_constraint_id)
                .map_err(|error| error.to_string())?;
            println!(
                "created foreign key {} (operation {}, commit {})",
                lower_hex(&result.foreign_key_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
        "foreign-key-drop" => {
            let foreign_key_id = parse_id(&take_text(fields, "--foreign-key-id")?)?;
            reject_unknown(fields)?;
            let result = client
                .drop_foreign_key(foreign_key_id)
                .map_err(|error| error.to_string())?;
            print_operation(
                "dropped foreign key",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        _ => return Err(format!("unsupported foreign-key command {command}")),
    }
    Ok(())
}

fn check_constraint_admin(
    command: &str,
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    match command {
        "check-create" => {
            let table_id = parse_id(&take_text(fields, "--table-id")?)?;
            let expression_bytes =
                parse_lowercase_hex_bytes(&take_text(fields, "--expression-eir1")?)?;
            let expression = prdb_engine::expression::decode_expression(&expression_bytes)
                .map_err(|_| "--expression-eir1 must contain a valid EIR1 expression".to_owned())?;
            let dependency_ids =
                parse_optional_constraint_columns(&take_text(fields, "--columns")?)?;
            reject_unknown(fields)?;
            let result = client
                .create_check_constraint(table_id, &expression, &dependency_ids)
                .map_err(|error| error.to_string())?;
            println!(
                "created CHECK constraint {} with expression {} (operation {}, commit {})",
                lower_hex(&result.constraint_id),
                lower_hex(&result.expression_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
        "check-drop" => {
            let constraint_id = parse_id(&take_text(fields, "--constraint-id")?)?;
            reject_unknown(fields)?;
            let result = client
                .drop_check_constraint(constraint_id)
                .map_err(|error| error.to_string())?;
            print_operation(
                "dropped CHECK constraint",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        _ => return Err(format!("unsupported CHECK command {command}")),
    }
    Ok(())
}

fn parse_id_list(value: &str, option: &str) -> Result<Vec<[u8; 16]>, String> {
    split_names(value, option)?
        .iter()
        .map(|value| {
            parse_id(value)
                .map_err(|_| format!("{option} must contain nonzero lowercase hexadecimal IDs"))
        })
        .collect()
}

fn take_optional_boolean(
    fields: &mut HashMap<String, std::ffi::OsString>,
    option: &str,
) -> Result<Option<bool>, String> {
    fields
        .remove(option)
        .map(|value| {
            value
                .into_string()
                .map_err(|_| format!("{option} must be UTF-8"))
                .and_then(|value| parse_boolean(&value, option))
        })
        .transpose()
}

fn parse_added_column(value: &str) -> Result<AdminColumnDefinition, String> {
    let mut columns = parse_column_definitions(value)?;
    if columns.len() != 1 || !columns[0].nullable || columns[0].generated_uuid_identity {
        return Err("column-add accepts one nullable column without identity".to_owned());
    }
    Ok(columns.remove(0))
}

fn parse_column_default(
    value: &str,
    scalar_type: ScalarType,
) -> Result<AdminColumnDefault, String> {
    if value == "null" {
        return Ok(AdminColumnDefault::Null);
    }
    if value == "current-principal-id" {
        if scalar_type == ScalarType::Uuid {
            return Ok(AdminColumnDefault::CurrentPrincipalId);
        }
        return Err("current-principal-id defaults require type uuid".to_owned());
    }
    let invalid = || format!("default value {value:?} does not match the declared type");
    let literal = match scalar_type {
        ScalarType::Boolean => ScalarValue::Boolean(parse_boolean(value, "--value")?),
        ScalarType::SmallInt => ScalarValue::SmallInt(value.parse().map_err(|_| invalid())?),
        ScalarType::Integer => ScalarValue::Integer(value.parse().map_err(|_| invalid())?),
        ScalarType::BigInt => ScalarValue::BigInt(value.parse().map_err(|_| invalid())?),
        ScalarType::Decimal { precision, scale } => ScalarValue::Decimal(
            parse_decimal_coefficient(value, precision, scale).ok_or_else(invalid)?,
        ),
        ScalarType::Text => ScalarValue::Text(value.to_owned()),
        ScalarType::Varchar { max_chars } => {
            if value.chars().count() > max_chars as usize {
                return Err(invalid());
            }
            ScalarValue::Varchar(value.to_owned())
        }
        ScalarType::Bytea => ScalarValue::Bytea(parse_hex_bytes(value).ok_or_else(invalid)?),
        ScalarType::Uuid => ScalarValue::Uuid(parse_uuid_literal(value).ok_or_else(invalid)?),
        ScalarType::Date => ScalarValue::Date(value.parse().map_err(|_| invalid())?),
        ScalarType::TimestampTz => ScalarValue::TimestampTz(value.parse().map_err(|_| invalid())?),
    };
    Ok(AdminColumnDefault::Literal(literal))
}

fn parse_decimal_coefficient(value: &str, precision: u8, scale: u8) -> Option<i128> {
    let (negative, unsigned) = value
        .strip_prefix('-')
        .map_or((false, value), |rest| (true, rest));
    if unsigned.is_empty() || unsigned.starts_with('+') {
        return None;
    }
    let (whole, fraction) = unsigned.split_once('.').unwrap_or((unsigned, ""));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
        || fraction.len() > usize::from(scale)
    {
        return None;
    }
    let mut digits = String::with_capacity(whole.len() + usize::from(scale));
    digits.push_str(whole);
    digits.push_str(fraction);
    digits.extend(std::iter::repeat_n(
        '0',
        usize::from(scale) - fraction.len(),
    ));
    let magnitude = digits.parse::<u128>().ok()?;
    if magnitude >= 10u128.checked_pow(u32::from(precision))? {
        return None;
    }
    let coefficient = i128::try_from(magnitude).ok()?;
    Some(if negative { -coefficient } else { coefficient })
}

fn parse_hex_bytes(value: &str) -> Option<Vec<u8>> {
    let digits = value.strip_prefix("hex:")?;
    if digits.len() % 2 != 0
        || !digits
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    digits
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| Some((nibble(pair[0]).ok()? << 4) | nibble(pair[1]).ok()?))
        .collect()
}

fn parse_uuid_literal(value: &str) -> Option<[u8; 16]> {
    if value.len() != 32
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let mut id = [0; 16];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        id[index] = (nibble(pair[0]).ok()? << 4) | nibble(pair[1]).ok()?;
    }
    Some(id)
}

fn parse_column_definitions(value: &str) -> Result<Vec<AdminColumnDefinition>, String> {
    if value.is_empty() {
        return Err("--columns must contain at least one column".to_owned());
    }
    let mut columns = Vec::new();
    for definition in value.split(';') {
        let parts = definition.split(':').collect::<Vec<_>>();
        if !(3..=4).contains(&parts.len()) {
            return Err("each column must be name:type:nullable|not-null[:identity]".to_owned());
        }
        let name = parts[0].trim();
        let scalar_type = parse_scalar_type(parts[1].trim())?;
        let nullable = match parts[2].trim() {
            "nullable" => true,
            "not-null" => false,
            _ => return Err("column nullability must be nullable or not-null".to_owned()),
        };
        let generated_uuid_identity = match parts.get(3).map(|part| part.trim()) {
            None => false,
            Some("identity") => true,
            Some(_) => return Err("the only supported column modifier is identity".to_owned()),
        };
        if !nullable && !generated_uuid_identity {
            return Err("not-null columns require the identity modifier in this CLI".to_owned());
        }
        if generated_uuid_identity && scalar_type != ScalarType::Uuid {
            return Err("identity columns must have type uuid".to_owned());
        }
        if generated_uuid_identity && nullable {
            return Err("identity columns must be not-null".to_owned());
        }
        columns.push(AdminColumnDefinition {
            name: name.to_owned(),
            scalar_type,
            nullable,
            generated_uuid_identity,
            default: None,
        });
    }
    if columns.len() > prdb_engine::tuple::MAX_COLUMNS {
        return Err("--columns exceeds the maximum column count".to_owned());
    }
    let names = columns
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    if names.iter().any(|name| name.is_empty()) {
        return Err("column names must be nonempty".to_owned());
    }
    let mut unique = names.clone();
    unique.sort_unstable();
    unique.dedup();
    if unique.len() != names.len() {
        return Err("--columns contains duplicate names".to_owned());
    }
    Ok(columns)
}

fn parse_scalar_type(value: &str) -> Result<ScalarType, String> {
    let invalid = || format!("unsupported column type {value:?}");
    if let Some(parameters) = value
        .strip_prefix("decimal(")
        .and_then(|v| v.strip_suffix(')'))
    {
        let (precision, scale) = parameters.split_once(',').ok_or_else(invalid)?;
        if scale.contains(',') {
            return Err(invalid());
        }
        let precision = precision.parse::<u8>().map_err(|_| invalid())?;
        let scale = scale.parse::<u8>().map_err(|_| invalid())?;
        if !(1..=38).contains(&precision) || scale > precision {
            return Err(invalid());
        }
        return Ok(ScalarType::Decimal { precision, scale });
    }
    if let Some(length) = value
        .strip_prefix("varchar(")
        .and_then(|v| v.strip_suffix(')'))
    {
        let max_chars = length.parse::<u32>().map_err(|_| invalid())?;
        if max_chars == 0 {
            return Err(invalid());
        }
        return Ok(ScalarType::Varchar { max_chars });
    }
    match value {
        "boolean" => Ok(ScalarType::Boolean),
        "smallint" => Ok(ScalarType::SmallInt),
        "integer" => Ok(ScalarType::Integer),
        "bigint" => Ok(ScalarType::BigInt),
        "text" => Ok(ScalarType::Text),
        "bytea" => Ok(ScalarType::Bytea),
        "uuid" => Ok(ScalarType::Uuid),
        "date" => Ok(ScalarType::Date),
        "timestamptz" => Ok(ScalarType::TimestampTz),
        _ => Err(invalid()),
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

fn cancel(
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
    let result = client
        .cancel(invocation_id)
        .map_err(|error| error.to_string())?;
    println!("{}", cancellation_result_name(result));
    Ok(())
}

fn admin_cancel(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let invocation_id = parse_id(&take_text(fields, "--invocation-id")?)?;
    reject_unknown(fields)?;
    let mut credentials =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        credentials.token_id,
        &mut credentials.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let result = client
        .cancel_invocation(invocation_id)
        .map_err(|error| error.to_string())?;
    println!("{}", cancellation_result_name(result));
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

fn identity_admin(
    command: &str,
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    match command {
        "user-create" | "role-create" => {
            let name = take_text(fields, "--name")?;
            reject_unknown(fields)?;
            let result = if command == "user-create" {
                client.create_user(&name)
            } else {
                client.create_role(&name)
            }
            .map_err(|error| error.to_string())?;
            let kind = if command == "user-create" {
                "user"
            } else {
                "role"
            };
            println!(
                "created {kind} {} (operation {}, commit {})",
                lower_hex(&result.identity_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
        "user-rename" | "role-rename" => {
            let id = parse_id(&take_text(fields, "--id")?)?;
            let name = take_text(fields, "--name")?;
            reject_unknown(fields)?;
            let result = if command == "user-rename" {
                client.rename_user(id, &name)
            } else {
                client.rename_role(id, &name)
            }
            .map_err(|error| error.to_string())?;
            print_operation(
                "renamed identity",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        "user-drop" | "role-drop" => {
            let id = parse_id(&take_text(fields, "--id")?)?;
            reject_unknown(fields)?;
            let result = if command == "user-drop" {
                client.drop_user(id)
            } else {
                client.drop_role(id)
            }
            .map_err(|error| error.to_string())?;
            print_operation(
                "dropped identity",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        "user-login" => {
            let user_id = parse_id(&take_text(fields, "--user-id")?)?;
            let enabled = parse_boolean(&take_text(fields, "--enabled")?, "--enabled")?;
            reject_unknown(fields)?;
            let result = client
                .set_user_login(user_id, enabled)
                .map_err(|error| error.to_string())?;
            print_operation(
                "changed user login",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        "role-grant" | "role-revoke" => {
            let member_id = parse_id(&take_text(fields, "--member-id")?)?;
            let role_id = parse_id(&take_text(fields, "--role-id")?)?;
            reject_unknown(fields)?;
            let result = if command == "role-grant" {
                client.grant_role(member_id, role_id)
            } else {
                client.revoke_role(member_id, role_id)
            }
            .map_err(|error| error.to_string())?;
            print_operation(
                "changed role membership",
                &result.operation_id,
                result.commit_sequence,
            );
        }
        _ => return Err(format!("unsupported identity command {command}")),
    }
    Ok(())
}

fn print_operation(label: &str, operation_id: &[u8; 16], commit_sequence: u64) {
    println!(
        "{label} (operation {}, commit {commit_sequence})",
        lower_hex(operation_id)
    );
}

fn parse_boolean(value: &str, option: &str) -> Result<bool, String> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("{option} must be true or false")),
    }
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

fn cancellation_result_name(result: CancellationResult) -> &'static str {
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
    fn key_constraint_columns_require_ordered_distinct_nonzero_ids() {
        assert_eq!(
            parse_constraint_columns(
                "00000000000000000000000000000001,00000000000000000000000000000002"
            )
            .unwrap(),
            vec![
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2],
            ]
        );
        assert!(parse_constraint_columns("").is_err());
        assert!(
            parse_constraint_columns(
                "00000000000000000000000000000001,00000000000000000000000000000001"
            )
            .is_err()
        );
        assert!(parse_constraint_columns("00000000000000000000000000000000").is_err());
    }

    #[test]
    fn system_privilege_parser_accepts_only_the_closed_wire_enum() {
        assert_eq!(
            parse_system_privilege("manage_users").unwrap(),
            AdminSystemPrivilege::ManageUsers
        );
        assert_eq!(
            parse_system_privilege("manage_roles").unwrap(),
            AdminSystemPrivilege::ManageRoles
        );
        assert_eq!(
            parse_system_privilege("create_schema").unwrap(),
            AdminSystemPrivilege::CreateSchema
        );
        assert!(parse_system_privilege("MANAGE_USERS").is_err());
        assert!(parse_system_privilege("manage_tables").is_err());
    }

    #[test]
    fn object_privilege_parser_accepts_only_catalog_privileges() {
        for name in [
            "usage",
            "select",
            "insert",
            "update",
            "delete",
            "references",
            "create",
            "alter",
            "index",
            "manage_policies",
            "manage_grants",
        ] {
            assert!(parse_object_privilege(name).is_ok(), "{name}");
        }
        assert!(parse_object_privilege("all").is_err());
        assert!(parse_object_privilege("REFERENCES").is_err());
    }

    #[test]
    fn policy_recipient_parser_accepts_only_canonical_user_and_role_ids() {
        assert_eq!(
            parse_policy_recipients(
                "user:000102030405060708090a0b0c0d0e0f,role:101112131415161718191a1b1c1d1e1f"
            )
            .unwrap(),
            vec![
                AdminPolicyRecipient::User([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15]),
                AdminPolicyRecipient::Role([
                    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31,
                ]),
            ]
        );
        assert!(parse_policy_recipients("").is_err());
        assert!(parse_policy_recipients("group:000102030405060708090a0b0c0d0e0f").is_err());
        assert!(parse_policy_recipients("user:000102030405060708090a0b0c0d0e0A").is_err());
        assert!(parse_policy_recipients("user:00000000000000000000000000000000").is_err());
    }

    #[test]
    fn check_expression_hex_parser_is_canonical_and_dependencies_may_be_empty() {
        assert_eq!(
            parse_lowercase_hex_bytes("0102ff").unwrap(),
            vec![1, 2, 255]
        );
        assert!(parse_lowercase_hex_bytes("").is_err());
        assert!(parse_lowercase_hex_bytes("0").is_err());
        assert!(parse_lowercase_hex_bytes("0A").is_err());
        assert!(parse_lowercase_hex_bytes("0g").is_err());
        assert!(parse_optional_constraint_columns("").unwrap().is_empty());
        assert_eq!(
            parse_optional_constraint_columns("00000000000000000000000000000001").unwrap(),
            vec![[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]]
        );
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

    #[test]
    fn login_switch_parser_accepts_only_canonical_booleans() {
        assert!(parse_boolean("true", "--enabled").unwrap());
        assert!(!parse_boolean("false", "--enabled").unwrap());
        assert!(parse_boolean("yes", "--enabled").is_err());
        assert!(parse_boolean("TRUE", "--enabled").is_err());
    }

    #[test]
    fn table_column_parser_validates_types_nullability_and_names() {
        let columns = parse_column_definitions(
            "id:uuid:not-null:identity;name:varchar(80):nullable;amount:decimal(12,2):nullable",
        )
        .unwrap();
        assert_eq!(columns.len(), 3);
        assert!(columns[0].generated_uuid_identity);
        assert_eq!(
            columns[1].scalar_type,
            ScalarType::Varchar { max_chars: 80 }
        );
        assert_eq!(
            columns[2].scalar_type,
            ScalarType::Decimal {
                precision: 12,
                scale: 2
            }
        );
        assert!(parse_column_definitions("id:integer:nullable;id:text:nullable").is_err());
        assert!(parse_column_definitions("id:integer:not-null").is_err());
        assert!(parse_column_definitions("id:text:nullable:identity").is_err());
        assert!(parse_column_definitions("id:uuid:nullable:identity").is_err());
        assert!(parse_column_definitions("id:decimal(2,3):nullable").is_err());
        assert!(parse_column_definitions("id:varchar(0):nullable").is_err());
        assert!(parse_added_column("extra:integer:nullable").is_ok());
        assert!(parse_added_column("id:integer:not-null:identity").is_err());
        assert!(parse_added_column("one:text:nullable;two:text:nullable").is_err());
        assert_eq!(
            parse_column_default(
                "-12.30",
                ScalarType::Decimal {
                    precision: 5,
                    scale: 2
                }
            )
            .unwrap(),
            AdminColumnDefault::Literal(ScalarValue::Decimal(-1230))
        );
        assert_eq!(
            parse_column_default("current-principal-id", ScalarType::Uuid).unwrap(),
            AdminColumnDefault::CurrentPrincipalId
        );
        assert_eq!(
            parse_column_default("hex:00ff", ScalarType::Bytea).unwrap(),
            AdminColumnDefault::Literal(ScalarValue::Bytea(vec![0, 255]))
        );
        assert!(
            parse_column_default(
                "1.234",
                ScalarType::Decimal {
                    precision: 4,
                    scale: 2
                }
            )
            .is_err()
        );
        assert!(parse_column_default("current-principal-id", ScalarType::Text).is_err());
        assert_eq!(
            parse_id_list("00000000000000000000000000000001", "--columns").unwrap(),
            vec![[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]]
        );
        assert!(
            parse_id_list(
                "00000000000000000000000000000001,00000000000000000000000000000001",
                "--columns"
            )
            .is_err()
        );
    }

    #[test]
    fn table_owner_arguments_require_canonical_nonzero_ids_and_no_extras() {
        let mut fields = HashMap::from([
            (
                "--table-id".to_owned(),
                "000102030405060708090a0b0c0d0e0f".into(),
            ),
            (
                "--owner-id".to_owned(),
                "101112131415161718191a1b1c1d1e1f".into(),
            ),
        ]);
        assert_eq!(
            take_table_owner_args(&mut fields).unwrap(),
            (
                [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                [
                    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
                ],
            )
        );
        assert!(fields.is_empty());

        for owner in [
            "00000000000000000000000000000000",
            "000102030405060708090a0b0c0d0e0A",
            "000102030405060708090a0b0c0d0e0",
        ] {
            let mut fields = HashMap::from([
                (
                    "--table-id".to_owned(),
                    "000102030405060708090a0b0c0d0e0f".into(),
                ),
                ("--owner-id".to_owned(), owner.into()),
            ]);
            assert!(take_table_owner_args(&mut fields).is_err());
        }

        let mut fields = HashMap::from([
            (
                "--table-id".to_owned(),
                "000102030405060708090a0b0c0d0e0f".into(),
            ),
            (
                "--owner-id".to_owned(),
                "101112131415161718191a1b1c1d1e1f".into(),
            ),
            ("--unexpected".to_owned(), "value".into()),
        ]);
        assert!(take_table_owner_args(&mut fields).is_err());
    }

    #[test]
    fn schema_owner_arguments_require_canonical_nonzero_ids_and_no_extras() {
        let mut fields = HashMap::from([
            (
                "--schema-id".to_owned(),
                "000102030405060708090a0b0c0d0e0f".into(),
            ),
            (
                "--owner-id".to_owned(),
                "101112131415161718191a1b1c1d1e1f".into(),
            ),
        ]);
        assert_eq!(
            take_schema_owner_args(&mut fields).unwrap(),
            (
                [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
                [
                    16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
                ],
            )
        );
        assert!(fields.is_empty());

        for owner in [
            "00000000000000000000000000000000",
            "000102030405060708090a0b0c0d0e0A",
            "000102030405060708090a0b0c0d0e0",
        ] {
            let mut fields = HashMap::from([
                (
                    "--schema-id".to_owned(),
                    "000102030405060708090a0b0c0d0e0f".into(),
                ),
                ("--owner-id".to_owned(), owner.into()),
            ]);
            assert!(take_schema_owner_args(&mut fields).is_err());
        }

        let mut fields = HashMap::from([
            (
                "--schema-id".to_owned(),
                "000102030405060708090a0b0c0d0e0f".into(),
            ),
            (
                "--owner-id".to_owned(),
                "101112131415161718191a1b1c1d1e1f".into(),
            ),
            ("--unexpected".to_owned(), "value".into()),
        ]);
        assert!(take_schema_owner_args(&mut fields).is_err());
    }
}
