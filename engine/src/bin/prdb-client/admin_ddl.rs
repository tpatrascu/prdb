use prdb_engine::client_credentials::read_client_credentials;
use prdb_engine::client_submit::{
    AdminClient, AdminColumnDefault, AdminColumnDefinition, AdminCreatePolicyRequest,
    AdminObjectGrantTarget, AdminObjectPrivilege, AdminPolicyEffect, AdminPolicyKind,
    AdminPolicyOperation, AdminPolicyRecipient, AdminSystemPrivilege,
};
use prdb_engine::tuple::{ScalarType, ScalarValue};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;

use super::*;

pub(super) fn schema_table_admin(
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

pub(super) fn take_table_owner_args(
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<([u8; 16], [u8; 16]), String> {
    let table_id = parse_id(&take_text(fields, "--table-id")?)?;
    let owner_id = parse_id(&take_text(fields, "--owner-id")?)?;
    reject_unknown(fields)?;
    Ok((table_id, owner_id))
}

pub(super) fn take_schema_owner_args(
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<([u8; 16], [u8; 16]), String> {
    let schema_id = parse_id(&take_text(fields, "--schema-id")?)?;
    let owner_id = parse_id(&take_text(fields, "--owner-id")?)?;
    reject_unknown(fields)?;
    Ok((schema_id, owner_id))
}

pub(super) fn column_admin(
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

pub(super) fn index_admin(
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

pub(super) fn key_constraint_admin(
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

pub(super) fn parse_constraint_columns(value: &str) -> Result<Vec<[u8; 16]>, String> {
    let column_ids = parse_id_list(value, "--columns")?;
    if column_ids.is_empty() || column_ids.len() > prdb_engine::tuple::MAX_COLUMNS {
        return Err(format!(
            "--columns must contain between 1 and {} IDs",
            prdb_engine::tuple::MAX_COLUMNS
        ));
    }
    Ok(column_ids)
}

pub(super) fn parse_optional_constraint_columns(value: &str) -> Result<Vec<[u8; 16]>, String> {
    if value.is_empty() {
        return Ok(Vec::new());
    }
    parse_constraint_columns(value)
}

pub(super) fn parse_lowercase_hex_bytes(value: &str) -> Result<Vec<u8>, String> {
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

pub(super) fn parse_system_privilege(value: &str) -> Result<AdminSystemPrivilege, String> {
    match value {
        "manage_users" => Ok(AdminSystemPrivilege::ManageUsers),
        "manage_roles" => Ok(AdminSystemPrivilege::ManageRoles),
        "create_schema" => Ok(AdminSystemPrivilege::CreateSchema),
        _ => Err("--privilege must be manage_users, manage_roles, or create_schema".to_owned()),
    }
}

pub(super) fn system_privilege_admin(
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

pub(super) fn parse_object_privilege(value: &str) -> Result<AdminObjectPrivilege, String> {
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

pub(super) fn object_grant_admin(
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

pub(super) fn policy_admin(
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

pub(super) fn parse_policy_recipients(value: &str) -> Result<Vec<AdminPolicyRecipient>, String> {
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

pub(super) fn take_optional_policy_expression(
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

pub(super) fn foreign_key_admin(
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

pub(super) fn check_constraint_admin(
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

pub(super) fn parse_id_list(value: &str, option: &str) -> Result<Vec<[u8; 16]>, String> {
    split_names(value, option)?
        .iter()
        .map(|value| {
            parse_id(value)
                .map_err(|_| format!("{option} must contain nonzero lowercase hexadecimal IDs"))
        })
        .collect()
}

pub(super) fn take_optional_boolean(
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

pub(super) fn parse_added_column(value: &str) -> Result<AdminColumnDefinition, String> {
    let mut columns = parse_column_definitions(value)?;
    if columns.len() != 1 || !columns[0].nullable || columns[0].generated_uuid_identity {
        return Err("column-add accepts one nullable column without identity".to_owned());
    }
    Ok(columns.remove(0))
}

pub(super) fn parse_column_default(
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

pub(super) fn parse_decimal_coefficient(value: &str, precision: u8, scale: u8) -> Option<i128> {
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

pub(super) fn parse_hex_bytes(value: &str) -> Option<Vec<u8>> {
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

pub(super) fn parse_uuid_literal(value: &str) -> Option<[u8; 16]> {
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

pub(super) fn parse_column_definitions(value: &str) -> Result<Vec<AdminColumnDefinition>, String> {
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

pub(super) fn parse_scalar_type(value: &str) -> Result<ScalarType, String> {
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

pub(super) fn identity_admin(
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
