use prdb_engine::client_credentials::{read_client_credentials, write_client_credentials};
use prdb_engine::client_protocol::ClientCredentials;
use prdb_engine::client_submit::{AdminClient, SubmissionClient};
use std::collections::HashMap;
use std::fs::{self};
use std::io::{self};
use std::net::SocketAddr;
use std::path::PathBuf;

use super::*;

pub(super) fn status(
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

pub(super) fn cancel(
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

pub(super) fn admin_cancel(
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

pub(super) fn admin_status(
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
    let status = client
        .status(invocation_id)
        .map_err(|error| error.to_string())?;
    println!("{}", status_name(status));
    Ok(())
}

pub(super) fn token_issue(
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
    match fs::symlink_metadata(&output_path) {
        Ok(_) => {
            return Err(format!(
                "--output-credentials {} already exists; token issuance was not attempted",
                output_path.display()
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("cannot inspect credential output path: {error}")),
    }

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

pub(super) fn token_revoke(
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

pub(super) fn token_rotate(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let token_id = parse_id(&take_text(fields, "--token-id")?)?;
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
    match fs::symlink_metadata(&output_path) {
        Ok(_) => {
            return Err(format!(
                "--output-credentials {} already exists; token rotation was not attempted",
                output_path.display()
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(format!("cannot inspect credential output path: {error}")),
    }

    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let rotated = client
        .rotate_token(token_id, &label, expires_at_us)
        .map_err(|error| error.to_string())?;
    let new_token_id = rotated.token_id;
    let operation_id = rotated.operation_id;
    let new_credentials = ClientCredentials {
        token_id: new_token_id,
        token_secret: *rotated.token_secret(),
    };
    write_client_credentials(&output_path, &new_credentials).map_err(|error| {
        format!(
            "token rotation committed (operation {}, replacement token {}) but saving {} failed: {error}; revoke replacement token {} and issue a new credential",
            lower_hex(&operation_id),
            lower_hex(&new_token_id),
            output_path.display(),
            lower_hex(&new_token_id)
        )
    })?;
    println!(
        "saved replacement token {} to {} (operation {}, expires at {expires_at_us} us; old token {} revoked)",
        lower_hex(&new_token_id),
        output_path.display(),
        lower_hex(&operation_id),
        lower_hex(&token_id)
    );
    Ok(())
}

pub(super) fn token_list(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let user_id = parse_id(&take_text(fields, "--user-id")?)?;
    let after_token_id = fields
        .remove("--after-token-id")
        .map(|value| parse_id(&value.to_string_lossy()))
        .transpose()?;
    reject_unknown(fields)?;
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let page = client
        .list_tokens_page(user_id, after_token_id)
        .map_err(|error| error.to_string())?;
    print!("{{\"tokens\":[");
    for (index, token) in page.tokens.iter().enumerate() {
        if index != 0 {
            print!(",");
        }
        print!(
            "{{\"token_id\":{},\"label\":{},\"issued_at_us\":{},\"expires_at_us\":{},\"revoked\":{},\"revoked_at_us\":{}}}",
            json_string(&lower_hex(&token.token_id)),
            json_string(&token.label),
            token.issued_at_us,
            token.expires_at_us,
            token.revoked,
            token
                .revoked_at_us
                .map_or_else(|| "null".to_owned(), |value| value.to_string())
        );
    }
    match page.next_after_token_id {
        Some(cursor) => println!(
            "],\"next_after_token_id\":{}}}",
            json_string(&lower_hex(&cursor))
        ),
        None => println!("],\"next_after_token_id\":null}}"),
    }
    Ok(())
}

pub(super) fn dependency_list(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let object_id = parse_id(&take_text(fields, "--object-id")?)?;
    let page_limit = fields
        .remove("--limit")
        .map(|value| parse_number::<usize>(value, "--limit"))
        .transpose()?
        .unwrap_or(64);
    if !(1..=128).contains(&page_limit) {
        return Err("--limit must be between 1 and 128".to_owned());
    }
    let cursor = fields
        .remove("--cursor")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "--cursor must be UTF-8".to_owned())
        })
        .transpose()?;
    reject_unknown(fields)?;

    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let page = client
        .list_dependencies_page(object_id, page_limit, cursor.as_deref())
        .map_err(|error| error.to_string())?;
    print!("{{\"dependencies\":[");
    for (index, edge) in page.records.iter().enumerate() {
        if index != 0 {
            print!(",");
        }
        print!(
            "{{\"dependent_id\":{},\"referenced_id\":{},\"kind\":{}}}",
            json_string(&lower_hex(&edge.dependent_id)),
            json_string(&lower_hex(&edge.referenced_id)),
            json_string(edge.kind.as_wire_name())
        );
    }
    match page.next_cursor {
        Some(cursor) => println!("],\"next_cursor\":{}}}", json_string(&cursor)),
        None => println!("],\"next_cursor\":null}}"),
    }
    Ok(())
}

pub(super) fn constraint_list(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let schema_id = parse_id(&take_text(fields, "--schema-id")?)?;
    let table_id = parse_id(&take_text(fields, "--table-id")?)?;
    reject_unknown(fields)?;
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    print!("{{\"constraints\":[");
    let mut first_constraint = true;
    let mut cursor = None;
    loop {
        let page = client
            .list_constraints_page(schema_id, table_id, 128, cursor.as_deref())
            .map_err(|error| error.to_string())?;
        for constraint in &page.records {
            if !first_constraint {
                print!(",");
            }
            first_constraint = false;
            print!(
                "{{\"constraint_id\":{},\"kind\":{},\"column_ids\":[",
                json_string(&lower_hex(&constraint.constraint_id)),
                json_string(constraint.kind.as_wire_name())
            );
            for (column_index, column_id) in constraint.column_ids.iter().enumerate() {
                if column_index != 0 {
                    print!(",");
                }
                print!("{}", json_string(&lower_hex(column_id)));
            }
            print!("],\"referenced_table_id\":");
            match constraint.referenced_table_id {
                Some(referenced_table_id) => {
                    print!("{}", json_string(&lower_hex(&referenced_table_id)))
                }
                None => print!("null"),
            }
            print!(",\"referenced_column_ids\":[");
            for (column_index, column_id) in constraint.referenced_column_ids.iter().enumerate() {
                if column_index != 0 {
                    print!(",");
                }
                print!("{}", json_string(&lower_hex(column_id)));
            }
            print!("]}}");
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    println!("]}}");
    Ok(())
}

pub(super) fn index_list(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let schema_id = parse_id(&take_text(fields, "--schema-id")?)?;
    let table_id = parse_id(&take_text(fields, "--table-id")?)?;
    reject_unknown(fields)?;
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    print!("{{\"indexes\":[");
    let mut first_index = true;
    let mut cursor = None;
    let mut page_number = 1usize;
    loop {
        let page = client
            .list_indexes_page(schema_id, table_id, 128, cursor.as_deref())
            .map_err(|error| format!("index metadata page {page_number}: {error}"))?;
        for index in &page.records {
            if !first_index {
                print!(",");
            }
            first_index = false;
            print!(
                "{{\"index_id\":{},\"name\":{},\"column_ids\":[",
                json_string(&lower_hex(&index.index_id)),
                json_string(&index.name)
            );
            for (column_index, column_id) in index.column_ids.iter().enumerate() {
                if column_index != 0 {
                    print!(",");
                }
                print!("{}", json_string(&lower_hex(column_id)));
            }
            print!("],\"unique\":{}}}", index.unique);
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
        page_number = page_number.saturating_add(1);
    }
    println!("]}}");
    Ok(())
}

pub(super) fn grant_list(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let schema_id = parse_id(&take_text(fields, "--schema-id")?)?;
    let table_id = parse_id(&take_text(fields, "--table-id")?)?;
    reject_unknown(fields)?;
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    print!("{{\"grants\":[");
    let mut first_grant = true;
    let mut cursor = None;
    loop {
        let page = client
            .list_grants_page(schema_id, table_id, 128, cursor.as_deref())
            .map_err(|error| error.to_string())?;
        for grant in &page.records {
            if !first_grant {
                print!(",");
            }
            first_grant = false;
            print!(
                "{{\"grantee_id\":{},\"grantor_id\":{},\"privilege\":{},\"target_scope\":{},\"target_id\":{}}}",
                json_string(&lower_hex(&grant.grantee_id)),
                json_string(&lower_hex(&grant.grantor_id)),
                json_string(grant.privilege.as_wire_name()),
                json_string(grant.target_scope.as_wire_name()),
                json_string(&lower_hex(&grant.target_id)),
            );
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    println!("]}}");
    Ok(())
}

pub(super) fn policy_list(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let schema_id = parse_id(&take_text(fields, "--schema-id")?)?;
    let table_id = parse_id(&take_text(fields, "--table-id")?)?;
    reject_unknown(fields)?;
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    print!("{{\"policies\":[");
    let mut first_policy = true;
    let mut cursor = None;
    loop {
        let page = client
            .list_policies_page(schema_id, table_id, 128, cursor.as_deref())
            .map_err(|error| error.to_string())?;
        for policy in &page.records {
            if !first_policy {
                print!(",");
            }
            first_policy = false;
            let kind = match policy.kind {
                prdb_engine::metadata_discovery::AdministrativePolicyKind::Row => "row",
                prdb_engine::metadata_discovery::AdministrativePolicyKind::Cell => "cell",
            };
            let operation = match policy.operation {
                prdb_engine::metadata_discovery::AdministrativePolicyOperation::Select => "select",
                prdb_engine::metadata_discovery::AdministrativePolicyOperation::Insert => "insert",
                prdb_engine::metadata_discovery::AdministrativePolicyOperation::Update => "update",
                prdb_engine::metadata_discovery::AdministrativePolicyOperation::Delete => "delete",
            };
            let effect = match policy.effect {
                prdb_engine::metadata_discovery::AdministrativePolicyEffect::Allow => "allow",
                prdb_engine::metadata_discovery::AdministrativePolicyEffect::Restrict => "restrict",
            };
            print!(
                "{{\"policy_id\":{},\"kind\":{},\"operation\":{},\"effect\":{},\"column_ids\":[",
                json_string(&lower_hex(&policy.policy_id)),
                json_string(kind),
                json_string(operation),
                json_string(effect),
            );
            for (column_index, column_id) in policy.column_ids.iter().enumerate() {
                if column_index != 0 {
                    print!(",");
                }
                print!("{}", json_string(&lower_hex(column_id)));
            }
            print!("],\"recipients\":");
            match &policy.recipients {
                Some(recipients) => {
                    print!("[");
                    for (recipient_index, recipient) in recipients.iter().enumerate() {
                        if recipient_index != 0 {
                            print!(",");
                        }
                        let recipient_kind = match recipient.kind {
                            prdb_engine::metadata_discovery::AdministrativePolicyRecipientKind::User => "user",
                            prdb_engine::metadata_discovery::AdministrativePolicyRecipientKind::Role => "role",
                        };
                        print!(
                            "{{\"kind\":{},\"principal_id\":{}}}",
                            json_string(recipient_kind),
                            json_string(&lower_hex(&recipient.principal_id)),
                        );
                    }
                    print!("]}}");
                }
                None => print!("null}}"),
            }
        }
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    println!("]}}");
    Ok(())
}

pub(super) fn audit_list(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let page_limit = fields
        .remove("--limit")
        .map(|value| parse_number::<usize>(value, "--limit"))
        .transpose()?
        .unwrap_or(64);
    let cursor = fields
        .remove("--cursor")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "--cursor must be UTF-8".to_owned())
        })
        .transpose()?;
    reject_unknown(fields)?;
    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let page = client
        .list_audit_events_page(page_limit, cursor.as_deref())
        .map_err(|error| error.to_string())?;
    print!("{{\"events\":[");
    for (index, event) in page.events.iter().enumerate() {
        if index != 0 {
            print!(",");
        }
        print!(
            "{{\"event_id\":\"{}\",\"actor_id\":{},\"correlation_id\":\"{}\",\"occurred_at_us\":{},\"action\":{},\"result\":{},\"target_ids\":[",
            lower_hex(&event.event_id),
            event
                .actor_id
                .map(|id| format!("\"{}\"", lower_hex(&id)))
                .unwrap_or_else(|| "null".to_owned()),
            lower_hex(&event.correlation_id),
            event.occurred_at_us,
            event.action,
            event.result,
        );
        for (target_index, target_id) in event.target_ids.iter().enumerate() {
            if target_index != 0 {
                print!(",");
            }
            print!("\"{}\"", lower_hex(target_id));
        }
        print!("]}}");
    }
    println!(
        "],\"next_cursor\":{}}}",
        page.next_cursor
            .map(|value| format!("\"{value}\""))
            .unwrap_or_else(|| "null".to_owned())
    );
    Ok(())
}

pub(super) fn runtime_list(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let page_limit = fields
        .remove("--limit")
        .map(|value| parse_number::<usize>(value, "--limit"))
        .transpose()?
        .unwrap_or(64);
    if !(1..=prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE).contains(&page_limit) {
        return Err(format!(
            "--limit must be between 1 and {}",
            prdb_engine::storage_manager::MAX_ADMIN_INVOCATION_PAGE
        ));
    }
    let cursor = fields
        .remove("--cursor")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "--cursor must be UTF-8".to_owned())
        })
        .transpose()?;
    reject_unknown(fields)?;

    let mut administrator =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        administrator.token_id,
        &mut administrator.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let page = client
        .list_runtime_invocations_page(page_limit, cursor.as_deref())
        .map_err(|error| error.to_string())?;
    print!(
        "{{\"observed_at_us\":{},\"consistency\":\"best_effort\",\"invocations\":[",
        page.observed_at_us
    );
    for (index, invocation) in page.records.iter().enumerate() {
        if index != 0 {
            print!(",");
        }
        let state = match invocation.state {
            prdb_engine::client_submit::RuntimeInvocationState::Active => "active",
            prdb_engine::client_submit::RuntimeInvocationState::Committing => "committing",
            prdb_engine::client_submit::RuntimeInvocationState::Committed => "committed",
            prdb_engine::client_submit::RuntimeInvocationState::Aborted => "aborted",
        };
        print!(
            "{{\"invocation_id\":{},\"transaction_id\":{},\"principal_id\":{},\"state\":{},\"started_at_us\":{},\"age_us\":{},\"deadline_at_us\":{},\"staged_rows\":{},\"staged_value_bytes\":{},\"snapshot_pin_count\":{},\"cancellation_requested\":{},\"cancellation_reason_code\":{}}}",
            json_string(&lower_hex(&invocation.invocation_id)),
            json_string(&lower_hex(&invocation.transaction_id)),
            json_string(&lower_hex(&invocation.principal_id)),
            json_string(state),
            invocation
                .started_at_us
                .map_or_else(|| "null".to_owned(), |v| v.to_string()),
            invocation.age_us,
            invocation
                .deadline_at_us
                .map_or_else(|| "null".to_owned(), |v| v.to_string()),
            invocation.staged_rows,
            invocation.staged_value_bytes,
            invocation.snapshot_pin_count,
            invocation.cancellation_requested,
            invocation
                .cancellation_reason_code
                .map_or_else(|| "null".to_owned(), |v| v.to_string())
        );
    }
    print!(
        "],\"pinned_snapshot_count\":{},\"oldest_snapshot_pin_age_us\":{},\"invocations_admitted_total\":{},\"invocation_conflicts_total\":{},\"admin_cancellations_accepted_total\":{},\"admin_cancellations_denied_total\":{},\"invocation_policy_denials_total\":{},\"invocation_staged_value_bytes_total\":{},\"invocation_commit_latency_buckets\":[",
        page.pinned_snapshot_count,
        page.oldest_snapshot_pin_age_us,
        page.invocations_admitted_total,
        page.invocation_conflicts_total,
        page.admin_cancellations_accepted_total,
        page.admin_cancellations_denied_total,
        page.invocation_policy_denials_total,
        page.invocation_staged_value_bytes_total
    );
    for (index, bucket) in page.invocation_commit_latency_buckets.iter().enumerate() {
        if index != 0 {
            print!(",");
        }
        print!("{bucket}");
    }
    print!("],\"invocation_redb_write_acquisition_buckets\":[");
    for (index, bucket) in page
        .invocation_redb_write_acquisition_buckets
        .iter()
        .enumerate()
    {
        if index != 0 {
            print!(",");
        }
        print!("{bucket}");
    }
    print!(
        "],\"outcome_maintenance_passes_total\":{},\"outcome_maintenance_failures_total\":{},\"outcomes_pruned_total\":{},\"fence_waits\":[",
        page.outcome_maintenance_passes_total,
        page.outcome_maintenance_failures_total,
        page.outcomes_pruned_total
    );
    for (index, wait) in page.fence_waits.iter().enumerate() {
        if index != 0 {
            print!(",");
        }
        print!(
            "{{\"wait_id\":{},\"category\":{},\"waited_us\":{},\"resource_ids\":[",
            json_string(&lower_hex(&wait.wait_id)),
            json_string(match wait.category {
                prdb_engine::client_submit::RuntimeFenceWaitCategory::Writer => "writer",
                prdb_engine::client_submit::RuntimeFenceWaitCategory::Ddl => "ddl",
            }),
            wait.waited_us
        );
        for (resource_index, resource_id) in wait.resource_ids.iter().enumerate() {
            if resource_index != 0 {
                print!(",");
            }
            print!("{}", json_string(&lower_hex(resource_id)));
        }
        print!(
            "],\"resources_truncated\":{},\"blocker_ids\":[",
            wait.resources_truncated
        );
        for (blocker_index, blocker_id) in wait.blocker_ids.iter().enumerate() {
            if blocker_index != 0 {
                print!(",");
            }
            print!("{}", json_string(&lower_hex(blocker_id)));
        }
        print!("],\"blockers_truncated\":{}}}", wait.blockers_truncated);
    }
    print!(
        "],\"fence_waits_truncated\":{},\"gateway_result_bytes_relayed_total\":{},\"gateway_result_bytes_sent_to_clients_total\":{},\"supervisor_queue\":{{\"pending_submissions\":{},\"starting_submissions\":{},\"started_submissions\":{},\"queued_starts\":{},\"oldest_queued_start_wait_us\":{},\"start_wait_buckets\":{:?},\"reserved_submission_bytes\":{}}},",
        page.fence_waits_truncated,
        page.gateway_result_bytes_relayed_total,
        page.gateway_result_bytes_sent_to_clients_total,
        page.supervisor_queue.pending_submissions,
        page.supervisor_queue.starting_submissions,
        page.supervisor_queue.started_submissions,
        page.supervisor_queue.queued_starts,
        page.supervisor_queue.oldest_queued_start_wait_us,
        page.supervisor_queue.start_wait_buckets,
        page.supervisor_queue.reserved_submission_bytes
    );
    match page.next_cursor {
        Some(cursor) => println!("\"next_cursor\":{}}}", json_string(&cursor)),
        None => println!("\"next_cursor\":null}}"),
    }
    Ok(())
}

pub(super) fn invocation_list(
    address: SocketAddr,
    credentials_path: PathBuf,
    fields: &mut HashMap<String, std::ffi::OsString>,
) -> Result<(), String> {
    let page_limit = fields
        .remove("--limit")
        .map(|value| parse_number::<usize>(value, "--limit"))
        .transpose()?
        .unwrap_or(64);
    if !(1..=prdb_engine::storage_manager::MAX_OWN_INVOCATION_PAGE).contains(&page_limit) {
        return Err(format!(
            "--limit must be between 1 and {}",
            prdb_engine::storage_manager::MAX_OWN_INVOCATION_PAGE
        ));
    }
    let cursor = fields
        .remove("--cursor")
        .map(|value| {
            value
                .into_string()
                .map_err(|_| "--cursor must be UTF-8".to_owned())
        })
        .transpose()?;
    reject_unknown(fields)?;

    let mut credentials =
        read_client_credentials(credentials_path).map_err(|error| error.to_string())?;
    let mut client = AdminClient::connect_authenticated(
        address,
        credentials.token_id,
        &mut credentials.token_secret,
    )
    .map_err(|error| error.to_string())?;
    let page = client
        .list_own_invocations_page(page_limit, cursor.as_deref())
        .map_err(|error| error.to_string())?;
    print!(
        "{{\"observed_at_us\":{},\"consistency\":\"best_effort\",\"invocations\":[",
        page.observed_at_us
    );
    for (index, invocation) in page.records.iter().enumerate() {
        if index != 0 {
            print!(",");
        }
        let state = match invocation.state {
            prdb_engine::client_submit::RuntimeInvocationState::Active => "active",
            prdb_engine::client_submit::RuntimeInvocationState::Committing => "committing",
            _ => return Err("server returned a terminal invocation in the live page".into()),
        };
        print!(
            "{{\"invocation_id\":{},\"state\":{},\"started_at_us\":{},\"deadline_at_us\":{},\"cancellation_requested\":{},\"staged_rows\":{},\"staged_value_bytes\":{}}}",
            json_string(&lower_hex(&invocation.invocation_id)),
            json_string(state),
            invocation.started_at_us,
            invocation
                .deadline_at_us
                .map_or_else(|| "null".to_owned(), |value| value.to_string()),
            invocation.cancellation_requested,
            invocation.staged_rows,
            invocation.staged_value_bytes
        );
    }
    println!(
        "],\"next_cursor\":{}}}",
        page.next_cursor
            .map(|value| format!("\"{value}\""))
            .unwrap_or_else(|| "null".to_owned())
    );
    Ok(())
}

pub(super) fn json_string(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('"');
    for character in value.chars() {
        match character {
            '"' => encoded.push_str("\\\""),
            '\\' => encoded.push_str("\\\\"),
            '\n' => encoded.push_str("\\n"),
            '\r' => encoded.push_str("\\r"),
            '\t' => encoded.push_str("\\t"),
            character if character < ' ' => {
                use std::fmt::Write as _;
                write!(&mut encoded, "\\u{:04x}", character as u32).unwrap();
            }
            character => encoded.push(character),
        }
    }
    encoded.push('"');
    encoded
}
