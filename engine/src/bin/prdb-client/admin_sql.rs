use prdb_engine::client_credentials::read_client_credentials;
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_sql::read_sql_input;
use prdb_engine::client_submit::{AdminClient, InvocationStatus};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use zeroize::Zeroizing;

use super::*;

pub(super) fn admin_sql(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let sql_path = take_path(fields, "--sql-file")?;
    reject_unknown(fields)?;
    let sql_bytes = Zeroizing::new(read_sql_input(&sql_path).map_err(|error| error.to_string())?);
    let sql =
        std::str::from_utf8(&sql_bytes).map_err(|_| "SQL input must be valid UTF-8".to_owned())?;
    if !is_supported_admin_sql(sql) {
        return Err("invalid administrative SQL: unsupported statement".to_owned());
    }
    let credentials =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    admin_sql_with_credentials(address, credentials, &sql_bytes)
}

pub(super) fn admin_sql_with_credentials(
    address: SocketAddr,
    administrator: ClientCredentials,
    sql_bytes: &[u8],
) -> Result<(), String> {
    let token_id = administrator.token_id;
    let mut reconciliation_secret = Zeroizing::new(administrator.token_secret);
    match admin_sql_with_credentials_once(address, administrator, sql_bytes) {
        Ok(()) => Ok(()),
        Err(error) => {
            let Some(operation_id) = parse_uncertain_admin_operation_id(&error) else {
                return Err(error);
            };
            let mut client = AdminClient::connect_authenticated(
                address,
                token_id,
                &mut reconciliation_secret[..],
            )
            .map_err(|status_error| {
                format!(
                    "{error}; administrative status lookup failed for operation {} ({status_error})",
                    lower_hex(&operation_id)
                )
            })?;
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                match client.status(operation_id).map_err(|status_error| {
                    format!(
                        "{error}; administrative status lookup failed for operation {} ({status_error})",
                        lower_hex(&operation_id)
                    )
                })? {
                    InvocationStatus::Committed => {
                        eprintln!(
                            "administrative operation {} committed after reconnect",
                            lower_hex(&operation_id)
                        );
                        return Ok(());
                    }
                    InvocationStatus::Aborted => {
                        return Err(format!(
                            "administrative operation {} aborted after an uncertain reply",
                            lower_hex(&operation_id)
                        ));
                    }
                    InvocationStatus::Unknown | InvocationStatus::Expired => {
                        return Err(format!(
                            "administrative operation {} has no retained outcome after an uncertain reply",
                            lower_hex(&operation_id)
                        ));
                    }
                    InvocationStatus::Prepared
                    | InvocationStatus::Active
                    | InvocationStatus::Committing => {
                        if Instant::now() >= deadline {
                            return Err(format!(
                                "administrative operation {} remains unresolved; query admin status before continuing ({error})",
                                lower_hex(&operation_id)
                            ));
                        }
                        std::thread::sleep(Duration::from_millis(25));
                    }
                }
            }
        }
    }
}

pub(super) fn parse_uncertain_admin_operation_id(error: &str) -> Option<[u8; 16]> {
    let encoded =
        error.strip_prefix("administrative operation outcome is uncertain; operation_id=")?;
    parse_lowercase_hex_bytes(encoded).ok()?.try_into().ok()
}

pub(super) fn admin_sql_with_credentials_once(
    address: SocketAddr,
    mut administrator: ClientCredentials,
    sql_bytes: &[u8],
) -> Result<(), String> {
    let sql =
        std::str::from_utf8(sql_bytes).map_err(|_| "SQL input must be valid UTF-8".to_owned())?;
    let table_statement = prdb_engine::ddl_sql::parse_create_table(sql).ok();
    let create_index_statement = prdb_engine::ddl_sql::parse_create_index(sql).ok();
    let rebuild_index_statement = prdb_engine::ddl_sql::parse_rebuild_index(sql).ok();
    let alter_index_rebuild_statement = prdb_engine::ddl_sql::parse_alter_index_rebuild(sql).ok();
    let rename_index_statement = prdb_engine::ddl_sql::parse_rename_index(sql).ok();
    let create_identity_statement = prdb_engine::ddl_sql::parse_create_identity(sql).ok();
    let rename_identity_statement = prdb_engine::ddl_sql::parse_rename_identity(sql).ok();
    let drop_identity_statement = prdb_engine::ddl_sql::parse_drop_identity(sql).ok();
    let set_user_login_statement = prdb_engine::ddl_sql::parse_set_user_login(sql).ok();
    let role_membership_statement = prdb_engine::ddl_sql::parse_role_membership(sql).ok();
    let system_privilege_statement = prdb_engine::ddl_sql::parse_system_privilege(sql).ok();
    let object_privilege_statement = prdb_engine::ddl_sql::parse_object_privilege(sql).ok();
    let create_policy_statement = prdb_engine::ddl_sql::parse_create_policy(sql).ok();
    let drop_policy_statement = prdb_engine::ddl_sql::parse_drop_policy(sql).ok();
    let drop_index_statement = prdb_engine::ddl_sql::parse_drop_index(sql).ok();
    let schema_statement = prdb_engine::ddl_sql::parse_create_schema(sql).ok();
    let rename_schema_statement = prdb_engine::ddl_sql::parse_rename_schema(sql).ok();
    let schema_owner_statement = prdb_engine::ddl_sql::parse_schema_owner_transfer(sql).ok();
    let drop_schema_statement = prdb_engine::ddl_sql::parse_drop_schema(sql).ok();
    let rename_table_statement = prdb_engine::ddl_sql::parse_rename_table(sql).ok();
    let table_owner_statement = prdb_engine::ddl_sql::parse_table_owner_transfer(sql).ok();
    let drop_table_statement = prdb_engine::ddl_sql::parse_drop_table(sql).ok();
    let truncate_table_statement = prdb_engine::ddl_sql::parse_truncate_table(sql).ok();
    let add_key_constraint_statement = prdb_engine::ddl_sql::parse_add_key_constraint(sql).ok();
    let add_foreign_key_statement = prdb_engine::ddl_sql::parse_add_foreign_key(sql).ok();
    let drop_constraint_statement = prdb_engine::ddl_sql::parse_drop_constraint(sql).ok();
    let add_check_constraint_statement = prdb_engine::ddl_sql::parse_add_check_constraint(sql).ok();
    let alter_column_nullability_statement =
        prdb_engine::ddl_sql::parse_alter_column_nullability(sql).ok();
    let widen_varchar_statement = prdb_engine::ddl_sql::parse_widen_varchar(sql).ok();
    let alter_column_default_statement = prdb_engine::ddl_sql::parse_alter_column_default(sql).ok();
    let add_column_statement = prdb_engine::ddl_sql::parse_add_column(sql).ok();
    let rename_column_statement = prdb_engine::ddl_sql::parse_rename_column(sql).ok();
    let drop_column_statement = prdb_engine::ddl_sql::parse_drop_column(sql).ok();
    if !is_supported_admin_sql(sql) {
        return Err("invalid administrative SQL: unsupported statement".to_owned());
    }

    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    if let Some(statement) = create_index_statement {
        if statement.if_not_exists {
            match client
                .execute_create_index_if_not_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                Some(result) => println!(
                    "created index {} (operation {}, commit {})",
                    lower_hex(&result.index_id),
                    lower_hex(&result.operation_id),
                    result.commit_sequence
                ),
                None => println!("index already exists; no operation committed"),
            }
        } else {
            let result = client
                .execute_create_index_sql(sql)
                .map_err(|error| error.to_string())?;
            println!(
                "created index {} (operation {}, commit {})",
                lower_hex(&result.index_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
    } else if let Some(statement) = create_identity_statement {
        let kind = match statement.kind {
            prdb_engine::ddl_sql::CreateIdentityKind::User => "user",
            prdb_engine::ddl_sql::CreateIdentityKind::Role => "role",
        };
        if statement.if_not_exists {
            match client
                .execute_create_identity_if_not_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                Some(result) => println!(
                    "created {kind} {} (operation {}, commit {})",
                    lower_hex(&result.identity_id),
                    lower_hex(&result.operation_id),
                    result.commit_sequence
                ),
                None => println!("{kind} already exists; no operation committed"),
            }
        } else {
            let result = client
                .execute_create_identity_sql(sql)
                .map_err(|error| error.to_string())?;
            println!(
                "created {kind} {} (operation {}, commit {})",
                lower_hex(&result.identity_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
    } else if let Some(statement) = rename_identity_statement {
        let result = client
            .execute_rename_identity_sql(sql)
            .map_err(|error| error.to_string())?;
        let kind = match statement.kind {
            prdb_engine::ddl_sql::CreateIdentityKind::User => "user",
            prdb_engine::ddl_sql::CreateIdentityKind::Role => "role",
        };
        print_operation(
            &format!("renamed {kind}"),
            &result.operation_id,
            result.commit_sequence,
        );
    } else if let Some(statement) = drop_identity_statement {
        if statement.if_exists {
            match client
                .execute_drop_identity_if_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                Some(result) => {
                    let kind = match statement.kind {
                        prdb_engine::ddl_sql::CreateIdentityKind::User => "user",
                        prdb_engine::ddl_sql::CreateIdentityKind::Role => "role",
                    };
                    print_operation(
                        &format!("dropped {kind}"),
                        &result.operation_id,
                        result.commit_sequence,
                    );
                }
                None => println!("identity was absent; no operation committed"),
            }
        } else {
            let result = client
                .execute_drop_identity_sql(sql)
                .map_err(|error| error.to_string())?;
            let kind = match statement.kind {
                prdb_engine::ddl_sql::CreateIdentityKind::User => "user",
                prdb_engine::ddl_sql::CreateIdentityKind::Role => "role",
            };
            print_operation(
                &format!("dropped {kind}"),
                &result.operation_id,
                result.commit_sequence,
            );
        }
    } else if let Some(statement) = set_user_login_statement {
        let result = client
            .execute_set_user_login_sql(sql)
            .map_err(|error| error.to_string())?;
        let state = if statement.enabled {
            "LOGIN"
        } else {
            "NOLOGIN"
        };
        print_operation(
            &format!("set user {state}"),
            &result.operation_id,
            result.commit_sequence,
        );
    } else if let Some(statement) = role_membership_statement {
        let result = client
            .execute_role_membership_sql(sql)
            .map_err(|error| error.to_string())?;
        let action = if statement.grant {
            "granted"
        } else {
            "revoked"
        };
        print_operation(
            &format!("{action} role membership"),
            &result.operation_id,
            result.commit_sequence,
        );
    } else if let Some(statement) = system_privilege_statement {
        let result = client
            .execute_system_privilege_sql(sql)
            .map_err(|error| error.to_string())?;
        let action = if statement.grant {
            "granted"
        } else {
            "revoked"
        };
        print_operation(
            &format!("{action} system privilege"),
            &result.operation_id,
            result.commit_sequence,
        );
    } else if let Some(statement) = object_privilege_statement {
        let result = client
            .execute_object_privilege_sql(sql)
            .map_err(|error| error.to_string())?;
        let action = if statement.grant {
            "granted"
        } else {
            "revoked"
        };
        print_operation(
            &format!("{action} object privilege"),
            &result.operation_id,
            result.commit_sequence,
        );
    } else if create_policy_statement.is_some() {
        let result = client
            .execute_create_policy_sql(sql)
            .map_err(|error| error.to_string())?;
        println!(
            "created policy {} (operation {}, commit {})",
            lower_hex(&result.policy_id),
            lower_hex(&result.operation_id),
            result.commit_sequence
        );
    } else if let Some(statement) = drop_policy_statement {
        if statement.if_exists {
            match client
                .execute_drop_policy_if_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                Some(result) => {
                    let kind = if statement.is_cell { "cell" } else { "row" };
                    print_operation(
                        &format!("dropped {kind} policy"),
                        &result.operation_id,
                        result.commit_sequence,
                    );
                }
                None => println!("policy was absent; no operation committed"),
            }
        } else {
            let result = client
                .execute_drop_policy_sql(sql)
                .map_err(|error| error.to_string())?;
            let kind = if statement.is_cell { "cell" } else { "row" };
            print_operation(
                &format!("dropped {kind} policy"),
                &result.operation_id,
                result.commit_sequence,
            );
        }
    } else if let Some(statement) = drop_index_statement {
        if statement.if_exists {
            if let Some(result) = client
                .execute_drop_index_if_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                print_operation(
                    "dropped index",
                    &result.operation_id,
                    result.commit_sequence,
                );
            } else {
                println!("index was absent; no operation committed");
            }
        } else {
            let result = client
                .execute_drop_index_sql(sql)
                .map_err(|error| error.to_string())?;
            print_operation(
                "dropped index",
                &result.operation_id,
                result.commit_sequence,
            );
        }
    } else if alter_index_rebuild_statement.is_some() {
        let result = client
            .execute_alter_index_rebuild_sql(sql)
            .map_err(|error| error.to_string())?;
        print_operation(
            "rebuilt index definition",
            &result.operation_id,
            result.commit_sequence,
        );
    } else if rename_index_statement.is_some() {
        let result = client
            .execute_rename_index_sql(sql)
            .map_err(|error| error.to_string())?;
        print_operation(
            "renamed index",
            &result.operation_id,
            result.commit_sequence,
        );
    } else if rebuild_index_statement.is_some() {
        let result = client
            .execute_rebuild_index_sql(sql)
            .map_err(|error| error.to_string())?;
        print_operation(
            "rebuilt index",
            &result.operation_id,
            result.commit_sequence,
        );
    } else if let Some(statement) = table_statement {
        if statement.if_not_exists {
            match client
                .execute_create_table_if_not_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                Some(result) => println!(
                    "created table {} with {} columns (operation {}, commit {})",
                    lower_hex(&result.table_id),
                    statement.columns.len(),
                    lower_hex(&result.operation_id),
                    result.commit_sequence
                ),
                None => println!("table already exists; no operation committed"),
            }
        } else {
            let result = client
                .execute_create_table_sql(sql)
                .map_err(|error| error.to_string())?;
            println!(
                "created table {} with {} columns (operation {}, commit {})",
                lower_hex(&result.table_id),
                statement.columns.len(),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
    } else if let Some(statement) = schema_statement {
        if statement.if_not_exists {
            match client
                .execute_create_schema_if_not_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                Some(result) => println!(
                    "created schema {} (operation {}, commit {})",
                    lower_hex(&result.identity_id),
                    lower_hex(&result.operation_id),
                    result.commit_sequence
                ),
                None => println!("schema already exists; no operation committed"),
            }
        } else {
            let result = client
                .execute_create_schema_sql(sql)
                .map_err(|error| error.to_string())?;
            println!(
                "created schema {} (operation {}, commit {})",
                lower_hex(&result.identity_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
    } else if rename_schema_statement.is_some() {
        let result = client
            .execute_rename_schema_sql(sql)
            .map_err(|error| error.to_string())?;
        println!(
            "renamed schema (operation {}, commit {})",
            lower_hex(&result.operation_id),
            result.commit_sequence
        );
    } else if schema_owner_statement.is_some() {
        let result = client
            .execute_schema_owner_transfer_sql(sql)
            .map_err(|error| error.to_string())?;
        print_operation(
            "changed schema owner",
            &result.operation_id,
            result.commit_sequence,
        );
    } else if let Some(statement) = drop_schema_statement {
        if statement.if_exists {
            match client
                .execute_drop_schema_if_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                Some(result) => println!(
                    "dropped schema (operation {}, commit {})",
                    lower_hex(&result.operation_id),
                    result.commit_sequence
                ),
                None => println!("schema did not exist"),
            }
        } else {
            let result = client
                .execute_drop_schema_sql(sql)
                .map_err(|error| error.to_string())?;
            println!(
                "dropped schema (operation {}, commit {})",
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
    } else if rename_table_statement.is_some() {
        let result = client
            .execute_rename_table_sql(sql)
            .map_err(|error| error.to_string())?;
        println!(
            "renamed table (operation {}, commit {})",
            lower_hex(&result.operation_id),
            result.commit_sequence
        );
    } else if table_owner_statement.is_some() {
        let result = client
            .execute_table_owner_transfer_sql(sql)
            .map_err(|error| error.to_string())?;
        print_operation(
            "changed table owner",
            &result.operation_id,
            result.commit_sequence,
        );
    } else if add_foreign_key_statement.is_some() {
        let result = client
            .execute_add_foreign_key_sql(sql)
            .map_err(|error| error.to_string())?;
        println!(
            "created FOREIGN KEY {} (operation {}, commit {})",
            lower_hex(&result.foreign_key_id),
            lower_hex(&result.operation_id),
            result.commit_sequence
        );
    } else if let Some(statement) = drop_constraint_statement {
        if statement.if_exists {
            match client
                .execute_drop_constraint_if_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                Some(result) => println!(
                    "dropped constraint (operation {}, commit {})",
                    lower_hex(&result.operation_id),
                    result.commit_sequence
                ),
                None => println!("constraint did not exist"),
            }
        } else {
            let result = client
                .execute_drop_constraint_sql(sql)
                .map_err(|error| error.to_string())?;
            println!(
                "dropped constraint (operation {}, commit {})",
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
    } else if add_check_constraint_statement.is_some() {
        let result = client
            .execute_add_check_constraint_sql(sql)
            .map_err(|error| error.to_string())?;
        println!(
            "created CHECK constraint {} (operation {}, commit {})",
            lower_hex(&result.constraint_id),
            lower_hex(&result.operation_id),
            result.commit_sequence
        );
    } else if let Some(statement) = widen_varchar_statement {
        let result = client
            .execute_widen_varchar_sql(sql)
            .map_err(|error| error.to_string())?;
        println!(
            "widened VARCHAR column {} to {} characters (operation {}, commit {})",
            statement.column,
            statement.max_chars,
            lower_hex(&result.operation_id),
            result.commit_sequence
        );
    } else if let Some(statement) = alter_column_nullability_statement {
        let result = client
            .execute_alter_column_nullability_sql(sql)
            .map_err(|error| error.to_string())?;
        println!(
            "{} NOT NULL on column {} (operation {}, commit {})",
            if statement.nullable { "dropped" } else { "set" },
            statement.column,
            lower_hex(&result.operation_id),
            result.commit_sequence
        );
    } else if let Some(statement) = alter_column_default_statement {
        let result = client
            .execute_alter_column_default_sql(sql)
            .map_err(|error| error.to_string())?;
        println!(
            "{} default on column {} (operation {}, commit {})",
            if statement.default.is_some() {
                "set"
            } else {
                "dropped"
            },
            statement.column,
            lower_hex(&result.operation_id),
            result.commit_sequence
        );
    } else if let Some(statement) = add_column_statement {
        if statement.if_not_exists {
            match client
                .execute_add_column_if_not_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                Some(result) => println!(
                    "added column {} (operation {}, commit {})",
                    lower_hex(&result.column_id),
                    lower_hex(&result.operation_id),
                    result.commit_sequence
                ),
                None => println!("column already exists; no operation committed"),
            }
        } else {
            let result = client
                .execute_add_column_sql(sql)
                .map_err(|error| error.to_string())?;
            println!(
                "added column {} (operation {}, commit {})",
                lower_hex(&result.column_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
    } else if rename_column_statement.is_some() {
        let result = client
            .execute_rename_column_sql(sql)
            .map_err(|error| error.to_string())?;
        println!(
            "renamed column {} (operation {}, commit {})",
            lower_hex(&result.column_id),
            lower_hex(&result.operation_id),
            result.commit_sequence
        );
    } else if let Some(statement) = drop_column_statement {
        if statement.if_exists {
            match client
                .execute_drop_column_if_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                Some(result) => println!(
                    "dropped column {} (operation {}, commit {})",
                    lower_hex(&result.column_id),
                    lower_hex(&result.operation_id),
                    result.commit_sequence
                ),
                None => println!("column was absent; no operation committed"),
            }
        } else {
            let result = client
                .execute_drop_column_sql(sql)
                .map_err(|error| error.to_string())?;
            println!(
                "dropped column {} (operation {}, commit {})",
                lower_hex(&result.column_id),
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
    } else if let Some(statement) = add_key_constraint_statement {
        let result = client
            .execute_add_key_constraint_sql(sql)
            .map_err(|error| error.to_string())?;
        println!(
            "created {} constraint {} with index {} (operation {}, commit {})",
            match statement.kind {
                prdb_engine::ddl_sql::KeyConstraintKind::PrimaryKey => "primary-key",
                prdb_engine::ddl_sql::KeyConstraintKind::Unique => "unique",
            },
            lower_hex(&result.constraint_id),
            lower_hex(&result.index_id),
            lower_hex(&result.operation_id),
            result.commit_sequence
        );
    } else if let Some(statement) = drop_table_statement {
        if statement.if_exists {
            match client
                .execute_drop_table_if_exists_sql(sql)
                .map_err(|error| error.to_string())?
            {
                Some(result) => println!(
                    "dropped table (operation {}, commit {})",
                    lower_hex(&result.operation_id),
                    result.commit_sequence
                ),
                None => println!("table did not exist"),
            }
        } else {
            let result = client
                .execute_drop_table_sql(sql)
                .map_err(|error| error.to_string())?;
            println!(
                "dropped table (operation {}, commit {})",
                lower_hex(&result.operation_id),
                result.commit_sequence
            );
        }
    } else if truncate_table_statement.is_some() {
        let result = client
            .execute_truncate_table_sql(sql)
            .map_err(|error| error.to_string())?;
        println!(
            "truncated table (operation {}, commit {})",
            lower_hex(&result.operation_id),
            result.commit_sequence
        );
    } else {
        return Err("invalid administrative SQL: unsupported statement".to_owned());
    }
    Ok(())
}

pub(super) fn is_supported_admin_sql(sql: &str) -> bool {
    prdb_engine::ddl_sql::parse_create_table(sql).is_ok()
        || prdb_engine::ddl_sql::parse_create_index(sql).is_ok()
        || prdb_engine::ddl_sql::parse_rebuild_index(sql).is_ok()
        || prdb_engine::ddl_sql::parse_alter_index_rebuild(sql).is_ok()
        || prdb_engine::ddl_sql::parse_rename_index(sql).is_ok()
        || prdb_engine::ddl_sql::parse_create_identity(sql).is_ok()
        || prdb_engine::ddl_sql::parse_rename_identity(sql).is_ok()
        || prdb_engine::ddl_sql::parse_drop_identity(sql).is_ok()
        || prdb_engine::ddl_sql::parse_set_user_login(sql).is_ok()
        || prdb_engine::ddl_sql::parse_role_membership(sql).is_ok()
        || prdb_engine::ddl_sql::parse_system_privilege(sql).is_ok()
        || prdb_engine::ddl_sql::parse_object_privilege(sql).is_ok()
        || prdb_engine::ddl_sql::parse_create_policy(sql).is_ok()
        || prdb_engine::ddl_sql::parse_drop_policy(sql).is_ok()
        || prdb_engine::ddl_sql::parse_drop_index(sql).is_ok()
        || prdb_engine::ddl_sql::parse_create_schema(sql).is_ok()
        || prdb_engine::ddl_sql::parse_rename_schema(sql).is_ok()
        || prdb_engine::ddl_sql::parse_schema_owner_transfer(sql).is_ok()
        || prdb_engine::ddl_sql::parse_drop_schema(sql).is_ok()
        || prdb_engine::ddl_sql::parse_rename_table(sql).is_ok()
        || prdb_engine::ddl_sql::parse_table_owner_transfer(sql).is_ok()
        || prdb_engine::ddl_sql::parse_drop_table(sql).is_ok()
        || prdb_engine::ddl_sql::parse_truncate_table(sql).is_ok()
        || prdb_engine::ddl_sql::parse_add_key_constraint(sql).is_ok()
        || prdb_engine::ddl_sql::parse_add_foreign_key(sql).is_ok()
        || prdb_engine::ddl_sql::parse_drop_constraint(sql).is_ok()
        || prdb_engine::ddl_sql::parse_add_check_constraint(sql).is_ok()
        || prdb_engine::ddl_sql::parse_alter_column_nullability(sql).is_ok()
        || prdb_engine::ddl_sql::parse_widen_varchar(sql).is_ok()
        || prdb_engine::ddl_sql::parse_alter_column_default(sql).is_ok()
        || prdb_engine::ddl_sql::parse_add_column(sql).is_ok()
        || prdb_engine::ddl_sql::parse_rename_column(sql).is_ok()
        || prdb_engine::ddl_sql::parse_drop_column(sql).is_ok()
}
