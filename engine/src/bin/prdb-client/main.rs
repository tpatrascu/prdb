use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

mod admin_ddl;
mod admin_sql;
mod args;
mod config;
mod inspect;
mod interactive;
mod submit_scope;
#[cfg(test)]
mod tests;

use admin_ddl::*;
use admin_sql::*;
use args::*;
use config::*;
use inspect::*;
use interactive::*;
use submit_scope::*;

const USAGE: &str = "Usage:\n\
  prdb-client instance list [--config FILE]\n\
  prdb-client instance show [--instance NAME] [--config FILE]\n\
  prdb-client instance set --instance NAME --address IP --port N [SDK options] \\\n    --default-credential PROFILE --username USER --credentials FILE \\\n    [--include-dir DIR --tcc FILE --shared-linker FILE --patchelf FILE] \\\n    [--make-default true|false] [--config FILE]\n\
  prdb-client sdk-library list|select|install|remove --instance NAME [options]\n\
  prdb-client submit --address IP:PORT --credentials FILE --package DIR --receipt FILE \\\n    [--parameters FILE] [--sql-data FILE] [--deadline-ms N] [--workload operational|analytical]\n\
  prdb-client interactive-sql --address LOOPBACK:PORT --credentials FILE --default-user USER \\
    [--credential-profile USER=FILE ...] [--sql-file FILE|-] --schema NAME --scope FILE \\
    [--parameters FILE] [--deadline-ms N] \\
    [--workload operational|analytical] (omit --sql-file for a terminal REPL)\n\\
    This submits SQL text directly; no artifact is compiled and no SDK/toolchain options apply.\n\\
  prdb-client admin-sql --address IP:PORT --credentials ADMIN_FILE --sql-file FILE|-\n\
  prdb-client status --address IP:PORT --credentials FILE --invocation-id HEX\n\n\
  prdb-client cancel --address IP:PORT --credentials FILE --invocation-id HEX\n\n\
  prdb-client admin-cancel --address IP:PORT --credentials ADMIN_FILE --invocation-id HEX\n\n\
  prdb-client admin-status --address IP:PORT --credentials ADMIN_FILE --invocation-id HEX\n\n\
  prdb-client invocation-list --address IP:PORT --credentials FILE\n    [--limit N] [--cursor HEX]\n\n\
  prdb-client scope --address IP:PORT --credentials FILE --schema NAME --output FILE \\\n    [--relation NAME --operations select,insert,update,delete [--columns NAME,...]]\n\n\
  prdb-client credential-remove --credentials FILE\n\n\
Submission output is provisional until a committed terminal event. START is never retried;\n\
if its reply is uncertain, use status with the invocation ID and a newly authenticated connection.\n\
Scope files contain only permissions visible to the authenticated identity and remain a request ceiling.\n\
Token secrets are saved only to mode-0600 credential files and are never printed.\n\
  prdb-client discover --address IP:PORT --credentials FILE [--schema NAME [--relation NAME]]\n\n\
  prdb-client token-issue --address IP:PORT --credentials ADMIN_FILE --user-id HEX\n\
    --label TEXT --ttl-seconds N --output-credentials FILE\n\
  prdb-client token-revoke --address IP:PORT --credentials ADMIN_FILE --token-id HEX\n\n\
  prdb-client token-rotate --address IP:PORT --credentials ADMIN_FILE --token-id HEX\n\
    --label TEXT --ttl-seconds N --output-credentials NEW_FILE\n\n\
  prdb-client token-list --address IP:PORT --credentials ADMIN_FILE --user-id HEX\n\
    [--after-token-id HEX]\n\n\
  prdb-client dependency-list --address IP:PORT --credentials ADMIN_FILE --object-id HEX\n\
    [--limit N] [--cursor HEX]\n\n\
  prdb-client constraint-list --address IP:PORT --credentials ADMIN_FILE --schema-id HEX --table-id HEX\n\
  prdb-client index-list --address IP:PORT --credentials ADMIN_FILE --schema-id HEX --table-id HEX\n\
  prdb-client grant-list --address IP:PORT --credentials ADMIN_FILE --schema-id HEX --table-id HEX\n\
  prdb-client policy-list --address IP:PORT --credentials ADMIN_FILE --schema-id HEX --table-id HEX\n\
  prdb-client runtime-list --address IP:PORT --credentials ADMIN_FILE\n\
    [--limit N] [--cursor HEX]\n\n\
  prdb-client audit-list --address IP:PORT --credentials ADMIN_FILE\n\
    [--limit N] [--cursor HEX]\n\n\
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
Token rotation revokes the old token and creates its replacement in one commit.";

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
    let mut command = command
        .into_string()
        .map_err(|_| "command must be UTF-8".to_owned())?;
    if command == "instance" || command == "sdk-library" {
        let subcommand = arguments
            .next()
            .ok_or_else(|| format!("missing {command} subcommand"))?
            .into_string()
            .map_err(|_| "subcommand must be UTF-8".to_owned())?;
        command = format!("{command} {subcommand}");
    }
    let mut fields = HashMap::<String, std::ffi::OsString>::new();
    let mut credential_profile_options = Vec::new();
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
        if command == "interactive-sql" && option == "--credential-profile" {
            credential_profile_options.push(value);
            continue;
        }
        if command == "instance set" && option == "--credential-profile" {
            credential_profile_options.push(value);
            continue;
        }
        if fields.insert(option.clone(), value).is_some() {
            return Err(format!("duplicate option {option}"));
        }
    }

    if command.starts_with("instance ") || command.starts_with("sdk-library ") {
        return run_client_config_command(&command, &mut fields, &credential_profile_options);
    }

    if command == "submit"
        && (fields.contains_key("--instance")
            || fields.contains_key("--config")
            || !fields.contains_key("--address")
            || !fields.contains_key("--credentials"))
    {
        return run_configured_submission(&mut fields);
    }

    if command == "interactive-sql"
        && (fields.contains_key("--instance")
            || fields.contains_key("--config")
            || !fields.contains_key("--address")
            || !fields.contains_key("--credentials"))
    {
        if !credential_profile_options.is_empty() {
            return Err("configure interactive credential profiles with instance set before using config mode".to_owned());
        }
        return run_configured_interactive(&mut fields);
    }

    if command == "credential-remove" {
        let credentials_path = take_path(&mut fields, "--credentials")?;
        reject_unknown(&fields)?;
        credential_remove(credentials_path)?;
        return Ok(());
    }

    if command == "interactive-sql" {
        let address = take_text(&mut fields, "--address")?
            .parse::<SocketAddr>()
            .map_err(|_| "--address must be a numeric IP:PORT socket address".to_owned())?;
        let credentials_path = take_path(&mut fields, "--credentials")?;
        let default_user = take_text(&mut fields, "--default-user")?;
        let mut additional_profiles = Vec::with_capacity(credential_profile_options.len());
        for profile in credential_profile_options {
            let profile = profile
                .into_string()
                .map_err(|_| "credential profile option must be UTF-8".to_owned())?;
            let (username, path) = profile
                .split_once('=')
                .ok_or_else(|| "--credential-profile must be USER=FILE".to_owned())?;
            if username.is_empty() || path.is_empty() {
                return Err("--credential-profile must be USER=FILE".to_owned());
            }
            additional_profiles.push((username.to_owned(), PathBuf::from(path)));
        }
        return interactive_sql(
            address,
            InteractiveClientOptions {
                default_credentials_path: credentials_path,
                default_user,
                additional_profiles,
                configured_profiles: None,
                repository_profile: false,
                selection_lock_context: None,
            },
            &mut fields,
        );
    }

    let address = take_text(&mut fields, "--address")?
        .parse::<SocketAddr>()
        .map_err(|_| "--address must be a numeric IP:PORT socket address".to_owned())?;
    let credentials_path = take_path(&mut fields, "--credentials")?;
    match command.as_str() {
        "submit" => submit(address, credentials_path, &mut fields),
        "admin-sql" => admin_sql(address, credentials_path, &mut fields),
        "status" => status(address, credentials_path, &mut fields),
        "cancel" => cancel(address, credentials_path, &mut fields),
        "admin-status" => admin_status(address, credentials_path, &mut fields),
        "admin-cancel" => admin_cancel(address, credentials_path, &mut fields),
        "invocation-list" => invocation_list(address, credentials_path, &mut fields),
        "scope" => scope(address, credentials_path, &mut fields),
        "discover" => discover(address, credentials_path, &mut fields),
        "token-issue" => token_issue(address, credentials_path, &mut fields),
        "token-revoke" => token_revoke(address, credentials_path, &mut fields),
        "token-rotate" => token_rotate(address, credentials_path, &mut fields),
        "token-list" => token_list(address, credentials_path, &mut fields),
        "dependency-list" => dependency_list(address, credentials_path, &mut fields),
        "constraint-list" => constraint_list(address, credentials_path, &mut fields),
        "index-list" => index_list(address, credentials_path, &mut fields),
        "grant-list" => grant_list(address, credentials_path, &mut fields),
        "policy-list" => policy_list(address, credentials_path, &mut fields),
        "runtime-list" => runtime_list(address, credentials_path, &mut fields),
        "audit-list" => audit_list(address, credentials_path, &mut fields),
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
