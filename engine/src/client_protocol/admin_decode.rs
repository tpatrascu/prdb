use super::json::*;
use super::types::*;
use super::*;
use std::collections::{HashMap, HashSet};

/// Decode the versioned, read-only ADMIN envelope and its first supported operation.
pub fn decode_client_admin_read(payload: &[u8]) -> Result<ClientAdminRead, ClientControlError> {
    match decode_client_admin_command(payload)? {
        ClientAdminCommand::ResolveIdentityName {
            name,
            kind,
            policy_scope,
        } => Ok(ClientAdminRead::ResolveIdentityName {
            name,
            kind,
            policy_scope,
        }),
        ClientAdminCommand::ResolveGrantIdentityName {
            schema_name,
            table_name,
            name,
            kind,
        } => Ok(ClientAdminRead::ResolveGrantIdentityName {
            schema_name,
            table_name,
            name,
            kind,
        }),
        ClientAdminCommand::ResolveAlterIdentityName {
            object_id,
            name,
            kind,
        } => Ok(ClientAdminRead::ResolveAlterIdentityName {
            object_id,
            name,
            kind,
        }),
        ClientAdminCommand::ResolvePolicyName {
            schema_id,
            table_id,
            name,
            is_cell,
        } => Ok(ClientAdminRead::ResolvePolicyName {
            schema_id,
            table_id,
            name,
            is_cell,
        }),
        ClientAdminCommand::ListSdkLibraries => Ok(ClientAdminRead::ListSdkLibraries),
        ClientAdminCommand::ListSdkLibrariesPage { page_limit, cursor } => {
            Ok(ClientAdminRead::ListSdkLibrariesPage { page_limit, cursor })
        }
        ClientAdminCommand::ListSchemas => Ok(ClientAdminRead::ListSchemas),
        ClientAdminCommand::ListSchemasPage { page_limit, cursor } => {
            Ok(ClientAdminRead::ListSchemasPage { page_limit, cursor })
        }
        ClientAdminCommand::ListRelations { schema_id } => {
            Ok(ClientAdminRead::ListRelations { schema_id })
        }
        ClientAdminCommand::ListRelationsPage {
            schema_id,
            page_limit,
            cursor,
        } => Ok(ClientAdminRead::ListRelationsPage {
            schema_id,
            page_limit,
            cursor,
        }),
        ClientAdminCommand::ListColumns {
            schema_id,
            relation_id,
        } => Ok(ClientAdminRead::ListColumns {
            schema_id,
            relation_id,
        }),
        ClientAdminCommand::ListColumnsPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        } => Ok(ClientAdminRead::ListColumnsPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        }),
        ClientAdminCommand::ListIndexesPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        } => Ok(ClientAdminRead::ListIndexesPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        }),
        ClientAdminCommand::ListConstraintsPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        } => Ok(ClientAdminRead::ListConstraintsPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        }),
        ClientAdminCommand::ListGrantsPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        } => Ok(ClientAdminRead::ListGrantsPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        }),
        ClientAdminCommand::ListPoliciesPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        } => Ok(ClientAdminRead::ListPoliciesPage {
            schema_id,
            relation_id,
            page_limit,
            cursor,
        }),
        ClientAdminCommand::ListDependenciesPage {
            object_id,
            page_limit,
            cursor,
        } => Ok(ClientAdminRead::ListDependenciesPage {
            object_id,
            page_limit,
            cursor,
        }),
        ClientAdminCommand::ListTokens {
            user_id,
            after_token_id,
        } => Ok(ClientAdminRead::ListTokens {
            user_id,
            after_token_id,
        }),
        ClientAdminCommand::ListAuditEvents { page_limit, cursor } => {
            Ok(ClientAdminRead::ListAuditEvents { page_limit, cursor })
        }
        _ => Err(ClientControlError::UnsupportedAdminOperation),
    }
}

/// Decode the currently supported ADMIN read and credential lifecycle commands.
pub fn decode_client_admin_command(
    payload: &[u8],
) -> Result<ClientAdminCommand, ClientControlError> {
    if payload.len() > CLIENT_MAX_CONTROL_BYTES {
        return Err(ClientControlError::InvalidJson);
    }
    let text = std::str::from_utf8(payload).map_err(|_| ClientControlError::InvalidJson)?;
    let mut parser = JsonStringObjectParser {
        text: text.as_bytes(),
        offset: 0,
    };
    parser.skip_whitespace();
    parser.expect(b'{')?;
    let mut phase = None;
    let mut version = None;
    let mut operation = None;
    let mut arguments = None;
    let mut table_columns = None;
    let mut primary_key_columns = None;
    let mut unique_constraints = None;
    let mut unique_constraint_names = None;
    let mut check_expressions = None;
    let mut check_constraint_names = None;
    let mut table_foreign_key_sources = None;
    let mut table_foreign_key_targets = None;
    let mut table_foreign_key_target_columns = None;
    let mut foreign_key_names = None;
    let mut foreign_key_source_columns = None;
    let mut constraint_column_ids = None;
    let mut index_unique = None;
    let mut check_dependency_column_ids = None;
    let mut policy_recipients = None;
    let mut policy_target_column_ids = None;
    let mut table_column = None;
    let mut column_default = None;
    let mut operation_id = None;
    let mut seen = std::collections::HashSet::new();
    loop {
        parser.skip_whitespace();
        if parser.consume(b'}') {
            break;
        }
        let key = parser.parse_string()?;
        if !seen.insert(key.clone()) {
            return Err(ClientControlError::DuplicateKey);
        }
        parser.skip_whitespace();
        parser.expect(b':')?;
        parser.skip_whitespace();
        match key.as_str() {
            "phase" => phase = Some(parser.parse_string()?),
            "operation_version" => version = Some(parser.parse_u64_number()?),
            "operation" => operation = Some(parser.parse_string()?),
            "operation_id" => operation_id = Some(parser.parse_string()?),
            "arguments" => {
                parser.expect(b'{')?;
                let mut args = HashMap::new();
                loop {
                    parser.skip_whitespace();
                    if parser.consume(b'}') {
                        break;
                    }
                    let arg = parser.parse_string()?;
                    if args.contains_key(&arg) {
                        return Err(ClientControlError::DuplicateKey);
                    }
                    parser.skip_whitespace();
                    parser.expect(b':')?;
                    parser.skip_whitespace();
                    if arg == "columns" && parser.text.get(parser.offset) == Some(&b'[') {
                        table_columns = Some(parser.parse_table_column_array()?);
                        args.insert(arg, "<structured-columns>".to_owned());
                    } else if arg == "primary_key_columns"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        primary_key_columns = Some(parser.parse_string_array()?);
                        args.insert(arg, "<structured-names>".to_owned());
                    } else if arg == "primary_key_name" {
                        args.insert(arg, parser.parse_string()?);
                    } else if arg == "unique_constraints"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        unique_constraints = Some(parser.parse_string_array_array()?);
                        args.insert(arg, "<structured-name-arrays>".to_owned());
                    } else if arg == "unique_constraint_names"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        unique_constraint_names = Some(parser.parse_string_array()?);
                        args.insert(arg, "<structured-unique-names>".to_owned());
                    } else if arg == "check_expressions_eir1"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        let encoded = parser.parse_string_array()?;
                        let expressions = encoded
                            .into_iter()
                            .map(|value| {
                                let bytes = decode_lower_hex_bytes(
                                    &value,
                                    crate::expression::MAX_EXPRESSION_BYTES,
                                )?;
                                let expression = crate::expression::decode_expression(&bytes)
                                    .map_err(|_| ClientControlError::InvalidPrepare)?;
                                if !crate::expression::is_row_local_expression(&expression) {
                                    return Err(ClientControlError::InvalidPrepare);
                                }
                                Ok(expression)
                            })
                            .collect::<Result<Vec<_>, ClientControlError>>()?;
                        check_expressions = Some(expressions);
                        args.insert(arg, "<structured-check-expressions>".to_owned());
                    } else if arg == "check_constraint_names"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        check_constraint_names = Some(parser.parse_string_array()?);
                        args.insert(arg, "<structured-check-names>".to_owned());
                    } else if arg == "foreign_key_source_columns"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        table_foreign_key_sources = Some(parser.parse_string_array_array()?);
                        args.insert(arg, "<structured-foreign-key-sources>".to_owned());
                    } else if arg == "foreign_key_target_constraint_ids"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        table_foreign_key_targets = Some(parser.parse_id_array_allow_zero()?);
                        args.insert(arg, "<structured-foreign-key-targets>".to_owned());
                    } else if arg == "foreign_key_target_columns"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        table_foreign_key_target_columns = Some(parser.parse_string_array_array()?);
                        args.insert(arg, "<structured-foreign-key-target-columns>".to_owned());
                    } else if arg == "foreign_key_names"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        foreign_key_names = Some(parser.parse_string_array()?);
                        args.insert(arg, "<structured-foreign-key-names>".to_owned());
                    } else if arg == "source_column_ids"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        foreign_key_source_columns = Some(parser.parse_id_array()?);
                        args.insert(arg, "<structured-ids>".to_owned());
                    } else if arg == "column_ids" && parser.text.get(parser.offset) == Some(&b'[') {
                        constraint_column_ids = Some(parser.parse_id_array()?);
                        args.insert(arg, "<structured-ids>".to_owned());
                    } else if arg == "dependency_column_ids"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        check_dependency_column_ids = Some(parser.parse_id_array()?);
                        args.insert(arg, "<structured-ids>".to_owned());
                    } else if arg == "target_column_ids"
                        && parser.text.get(parser.offset) == Some(&b'[')
                    {
                        policy_target_column_ids = Some(parser.parse_id_array()?);
                        args.insert(arg, "<structured-ids>".to_owned());
                    } else if arg == "target_ids" && parser.text.get(parser.offset) == Some(&b'[') {
                        let target_ids = parser.parse_string_array()?;
                        if target_ids.len() > 64 {
                            return Err(ClientControlError::InvalidPrepare);
                        }
                        args.insert(arg, target_ids.join(","));
                    } else if arg == "privileges" && parser.text.get(parser.offset) == Some(&b'[') {
                        let privileges = parser.parse_string_array()?;
                        if privileges.is_empty() || privileges.len() > 11 {
                            return Err(ClientControlError::InvalidPrepare);
                        }
                        args.insert(arg, privileges.join(","));
                    } else if arg == "recipients" && parser.text.get(parser.offset) == Some(&b'[') {
                        policy_recipients = Some(parser.parse_policy_recipient_array()?);
                        args.insert(arg, "<structured-recipients>".to_owned());
                    } else if arg == "unique" {
                        index_unique = Some(parser.parse_boolean()?);
                        args.insert(arg, "<boolean>".to_owned());
                    } else if arg == "column" && parser.text.get(parser.offset) == Some(&b'{') {
                        table_column = Some(parser.parse_table_column_definition()?);
                        args.insert(arg, "<structured-column>".to_owned());
                    } else if arg == "default" && parser.text.get(parser.offset) == Some(&b'{') {
                        column_default = Some(parser.parse_table_column_default()?);
                        args.insert(arg, "<structured-default>".to_owned());
                    } else if matches!(
                        arg.as_str(),
                        "using_expression_eir1" | "with_check_expression_eir1"
                    ) && parser.text.get(parser.offset..parser.offset + 4)
                        == Some(b"null")
                    {
                        parser.offset += 4;
                        args.insert(arg, "<null>".to_owned());
                    } else {
                        args.insert(arg, parser.parse_string()?);
                    }
                    parser.skip_whitespace();
                    if !parser.consume(b',') {
                        parser.expect(b'}')?;
                        break;
                    }
                    parser.skip_whitespace();
                    if parser.text.get(parser.offset) == Some(&b'}') {
                        return Err(ClientControlError::InvalidJson);
                    }
                }
                arguments = Some(args);
            }
            _ => return Err(ClientControlError::UnknownKey),
        }
        parser.skip_whitespace();
        if !parser.consume(b',') {
            parser.expect(b'}')?;
            break;
        }
        parser.skip_whitespace();
        if parser.text.get(parser.offset) == Some(&b'}') {
            return Err(ClientControlError::InvalidJson);
        }
    }
    parser.skip_whitespace();
    if parser.offset != parser.text.len() {
        return Err(ClientControlError::InvalidJson);
    }
    match phase.as_deref() {
        Some("read")
            if version == Some(1)
                && operation.as_deref() == Some("metadata.identities.resolve") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            let kind = match args.remove("kind").as_deref() {
                Some("user") => crate::metadata_discovery::AdministrativeIdentityKind::User,
                Some("role") => crate::metadata_discovery::AdministrativeIdentityKind::Role,
                Some("either") => crate::metadata_discovery::AdministrativeIdentityKind::Either,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ResolveIdentityName {
                name,
                kind,
                policy_scope: None,
            })
        }
        Some("read")
            if version == Some(1)
                && operation.as_deref() == Some("metadata.policy_identities.resolve") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            if args.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            let kind = match args.remove("kind").as_deref() {
                Some("user") => crate::metadata_discovery::AdministrativeIdentityKind::User,
                Some("role") => crate::metadata_discovery::AdministrativeIdentityKind::Role,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            if !args.is_empty() || schema_id == [0; 16] || table_id == [0; 16] {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::ResolveIdentityName {
                name,
                kind,
                policy_scope: Some((schema_id, table_id)),
            })
        }
        Some("read")
            if version == Some(1)
                && operation.as_deref() == Some("metadata.alter_identities.resolve") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            if args.len() != 3 {
                return Err(ClientControlError::UnknownKey);
            }
            let object_id = decode_lower_hex_id(
                &args
                    .remove("object_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            let kind = match args.remove("kind").as_deref() {
                Some("user") => crate::metadata_discovery::AdministrativeIdentityKind::User,
                Some("role") => crate::metadata_discovery::AdministrativeIdentityKind::Role,
                Some("either") => crate::metadata_discovery::AdministrativeIdentityKind::Either,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            if !args.is_empty() || object_id == [0; 16] {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::ResolveAlterIdentityName {
                object_id,
                name,
                kind,
            })
        }
        Some("read")
            if version == Some(1)
                && operation.as_deref() == Some("metadata.grant_identities.resolve") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            if !matches!(args.len(), 3 | 4) {
                return Err(ClientControlError::UnknownKey);
            }
            let schema_name = args
                .remove("schema_name")
                .ok_or(ClientControlError::MissingKey)?;
            let table_name = args.remove("table_name");
            if args.len() != 2 {
                return Err(ClientControlError::UnknownKey);
            }
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            let kind = match args.remove("kind").as_deref() {
                Some("user") => crate::metadata_discovery::AdministrativeIdentityKind::User,
                Some("role") => crate::metadata_discovery::AdministrativeIdentityKind::Role,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            if !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::ResolveGrantIdentityName {
                schema_name,
                table_name,
                name,
                kind,
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.policies.resolve") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            if args.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            let is_cell = match args.remove("kind").as_deref() {
                Some("row") => Some(false),
                Some("cell") => Some(true),
                Some("any") => None,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            if !args.is_empty() || schema_id == [0; 16] || table_id == [0; 16] {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::ResolvePolicyName {
                schema_id,
                table_id,
                name,
                is_cell,
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("sdk_libraries.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let pagination =
                decode_metadata_pagination(&mut args)?.ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListSdkLibrariesPage {
                page_limit: pagination.page_limit,
                cursor: pagination.cursor,
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.schemas.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let pagination = decode_metadata_pagination(&mut args)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(match pagination {
                Some(pagination) => ClientAdminCommand::ListSchemasPage {
                    page_limit: pagination.page_limit,
                    cursor: pagination.cursor,
                },
                None => ClientAdminCommand::ListSchemas,
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.relations.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let pagination = decode_metadata_pagination(&mut args)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(match pagination {
                Some(pagination) => ClientAdminCommand::ListRelationsPage {
                    schema_id,
                    page_limit: pagination.page_limit,
                    cursor: pagination.cursor,
                },
                None => ClientAdminCommand::ListRelations { schema_id },
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.columns.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let relation_id = decode_lower_hex_id(
                &args
                    .remove("relation_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let pagination = decode_metadata_pagination(&mut args)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(match pagination {
                Some(pagination) => ClientAdminCommand::ListColumnsPage {
                    schema_id,
                    relation_id,
                    page_limit: pagination.page_limit,
                    cursor: pagination.cursor,
                },
                None => ClientAdminCommand::ListColumns {
                    schema_id,
                    relation_id,
                },
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.indexes.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let relation_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let pagination =
                decode_metadata_pagination(&mut args)?.ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListIndexesPage {
                schema_id,
                relation_id,
                page_limit: pagination.page_limit,
                cursor: pagination.cursor,
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.constraints.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let relation_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let pagination =
                decode_metadata_pagination(&mut args)?.ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListConstraintsPage {
                schema_id,
                relation_id,
                page_limit: pagination.page_limit,
                cursor: pagination.cursor,
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.grants.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let relation_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let pagination =
                decode_metadata_pagination(&mut args)?.ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListGrantsPage {
                schema_id,
                relation_id,
                page_limit: pagination.page_limit,
                cursor: pagination.cursor,
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.policies.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let relation_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let pagination =
                decode_metadata_pagination(&mut args)?.ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListPoliciesPage {
                schema_id,
                relation_id,
                page_limit: pagination.page_limit,
                cursor: pagination.cursor,
            })
        }
        Some("read")
            if version == Some(1) && operation.as_deref() == Some("metadata.dependencies.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let object_id = decode_lower_hex_id(
                &args
                    .remove("object_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let pagination =
                decode_metadata_pagination(&mut args)?.ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListDependenciesPage {
                object_id,
                page_limit: pagination.page_limit,
                cursor: pagination.cursor,
            })
        }
        Some("read") if version == Some(1) && operation.as_deref() == Some("tokens.list") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let user_id = decode_lower_hex_id(
                &args
                    .remove("user_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let after_token_id = args
                .remove("after_token_id")
                .map(|value| decode_lower_hex_id(&value))
                .transpose()?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListTokens {
                user_id,
                after_token_id,
            })
        }
        Some("read") if version == Some(1) && operation.as_deref() == Some("audit.events.list") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let page_limit = args.remove("limit").ok_or(ClientControlError::MissingKey)?;
            let page_limit = parse_canonical_positive_i64(&page_limit)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value <= crate::storage_manager::MAX_ADMIN_AUDIT_PAGE)
                .ok_or(ClientControlError::InvalidPrepare)?;
            let cursor = args
                .remove("cursor")
                .map(|value| {
                    crate::storage_manager::AdministrativeAuditCursor::from_wire(&value)
                        .ok_or(ClientControlError::InvalidPrepare)
                })
                .transpose()?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListAuditEvents { page_limit, cursor })
        }
        Some("read")
            if matches!(version, Some(1..=8))
                && operation.as_deref() == Some("runtime.invocations.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let page_limit = args.remove("limit").ok_or(ClientControlError::MissingKey)?;
            let page_limit = parse_canonical_positive_i64(&page_limit)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value <= crate::storage_manager::MAX_ADMIN_INVOCATION_PAGE)
                .ok_or(ClientControlError::InvalidPrepare)?;
            let cursor = args
                .remove("cursor")
                .map(|value| {
                    crate::storage_manager::AdministrativeInvocationCursor::from_wire(&value)
                        .ok_or(ClientControlError::InvalidPrepare)
                })
                .transpose()?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListRuntimeInvocations {
                response_version: version.ok_or(ClientControlError::MissingKey)?,
                page_limit,
                cursor,
            })
        }
        Some("read")
            if matches!(version, Some(1 | 2))
                && operation.as_deref() == Some("runtime.invocations.own.list") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let page_limit = args.remove("limit").ok_or(ClientControlError::MissingKey)?;
            let page_limit = parse_canonical_positive_i64(&page_limit)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|value| *value <= crate::storage_manager::MAX_OWN_INVOCATION_PAGE)
                .ok_or(ClientControlError::InvalidPrepare)?;
            let cursor = args
                .remove("cursor")
                .map(|value| {
                    crate::storage_manager::InvocationListCursor::from_wire(&value)
                        .ok_or(ClientControlError::InvalidPrepare)
                })
                .transpose()?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::ListOwnInvocations {
                response_version: version.ok_or(ClientControlError::MissingKey)?,
                page_limit,
                cursor,
            })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("sdk_libraries.install") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let operation_id = decode_lower_hex_id(
                &args
                    .remove("operation_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if operation_id == [0; 16] {
                return Err(ClientControlError::InvalidPrepare);
            }
            let metadata_bytes = args
                .remove("metadata_bytes")
                .and_then(|value| parse_canonical_positive_i64(&value))
                .and_then(|value| u64::try_from(value).ok())
                .filter(|bytes| *bytes <= 64 * 1024)
                .ok_or(ClientControlError::InvalidPrepare)?;
            let library_bytes = args
                .remove("library_bytes")
                .and_then(|value| parse_canonical_positive_i64(&value))
                .and_then(|value| u64::try_from(value).ok())
                .filter(|bytes| *bytes <= 128 * 1024 * 1024)
                .ok_or(ClientControlError::InvalidPrepare)?;
            let library_sha256 = decode_lower_hex_hash(
                &args
                    .remove("library_sha256")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::BeginSdkLibraryInstall {
                operation_id,
                metadata_bytes,
                library_bytes,
                library_sha256,
            })
        }
        Some("write")
            if version == Some(1)
                && operation.as_deref() == Some("sdk_libraries.install.finish") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let operation_id = decode_lower_hex_id(
                &args
                    .remove("operation_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if operation_id == [0; 16] || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::FinishSdkLibraryInstall { operation_id })
        }
        Some("write")
            if version == Some(1) && operation.as_deref() == Some("sdk_libraries.remove") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let operation_id = decode_lower_hex_id(
                &args
                    .remove("operation_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let release = args
                .remove("release")
                .ok_or(ClientControlError::MissingKey)?;
            let sdk_release = parse_canonical_positive_i64(&release)
                .and_then(|value| u32::try_from(value).ok())
                .ok_or(ClientControlError::InvalidPrepare)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            if operation_id == [0; 16] {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::RemoveSdkLibrary {
                operation_id,
                sdk_release,
            })
        }
        Some("read")
            if version == Some(1)
                && operation.as_deref() == Some("sdk_libraries.operation_status") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let operation_id = decode_lower_hex_id(
                &args
                    .remove("operation_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if operation_id == [0; 16] || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::SdkLibraryOperationStatus { operation_id })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("users.create") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !is_canonical_admin_principal_name(&name) || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateUser { name })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("users.create_no_login") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !is_canonical_admin_principal_name(&name) || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateUserNoLogin { name })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("roles.create") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !is_canonical_admin_principal_name(&name) || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateRole { name })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("schemas.create") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            let owner_id = args
                .remove("owner_id")
                .map(|value| decode_lower_hex_id(&value))
                .transpose()?;
            if !is_canonical_admin_principal_name(&name) || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateSchema { name, owner_id })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("schemas.drop") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropSchema { schema_id })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("schemas.set_owner") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let owner_id = decode_lower_hex_id(
                &args
                    .remove("owner_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareSetSchemaOwner {
                schema_id,
                owner_id,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("schemas.rename") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() || !is_canonical_admin_principal_name(&name) {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareRenameSchema { schema_id, name })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("tables.drop") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropTable { table_id })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.truncate") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareTruncateTable { table_id })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("foreign_keys.drop") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let foreign_key_id = decode_lower_hex_id(
                &args
                    .remove("foreign_key_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropForeignKey { foreign_key_id })
        }
        Some("prepare")
            if matches!(version, Some(1 | 2))
                && operation.as_deref() == Some("foreign_keys.create") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let source_table_id = decode_lower_hex_id(
                &args
                    .remove("source_table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let target_constraint_id = decode_lower_hex_id(
                &args
                    .remove("target_constraint_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = if version == Some(2) {
                let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
                if !is_canonical_admin_principal_name(&name) {
                    return Err(ClientControlError::InvalidPrepare);
                }
                Some(name)
            } else {
                None
            };
            args.remove("source_column_ids")
                .ok_or(ClientControlError::MissingKey)?;
            let source_column_ids = foreign_key_source_columns
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if source_column_ids.is_empty() || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateForeignKey {
                source_table_id,
                source_column_ids,
                target_constraint_id,
                name,
            })
        }
        Some("prepare")
            if matches!(version, Some(1 | 2))
                && operation.as_deref() == Some("constraints.create_unique") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = if version == Some(2) {
                let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
                if !is_canonical_admin_principal_name(&name) {
                    return Err(ClientControlError::InvalidPrepare);
                }
                Some(name)
            } else {
                None
            };
            args.remove("column_ids")
                .ok_or(ClientControlError::MissingKey)?;
            let column_ids = constraint_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if column_ids.is_empty() || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateUniqueConstraint {
                table_id,
                column_ids,
                name,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("indexes.create") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if args.remove("column_ids").as_deref() != Some("<structured-ids>") {
                return Err(ClientControlError::MissingKey);
            }
            let column_ids = constraint_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if args.remove("unique").as_deref() != Some("<boolean>") || !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            let unique = index_unique.ok_or(ClientControlError::MissingKey)?;
            if !is_canonical_admin_principal_name(&name)
                || column_ids.is_empty()
                || column_ids
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != column_ids.len()
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateIndex {
                table_id,
                name,
                column_ids,
                unique,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("indexes.drop") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let index_id = decode_lower_hex_id(
                &args
                    .remove("index_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropIndex { index_id })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("indexes.rename") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let index_id = decode_lower_hex_id(
                &args
                    .remove("index_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() || !is_canonical_admin_principal_name(&name) {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareRenameIndex { index_id, name })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("indexes.rebuild") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let index_id = decode_lower_hex_id(
                &args
                    .remove("index_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if args.remove("column_ids").as_deref() != Some("<structured-ids>") {
                return Err(ClientControlError::MissingKey);
            }
            let column_ids = constraint_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if args.remove("unique").as_deref() != Some("<boolean>") || !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            let unique = index_unique.ok_or(ClientControlError::MissingKey)?;
            if column_ids.is_empty()
                || column_ids
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != column_ids.len()
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareRebuildIndex {
                index_id,
                column_ids,
                unique,
            })
        }
        Some("prepare")
            if matches!(version, Some(1 | 2))
                && operation.as_deref() == Some("constraints.create_primary_key") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = if version == Some(2) {
                let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
                if !is_canonical_admin_principal_name(&name) {
                    return Err(ClientControlError::InvalidPrepare);
                }
                Some(name)
            } else {
                None
            };
            args.remove("column_ids")
                .ok_or(ClientControlError::MissingKey)?;
            let column_ids = constraint_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if column_ids.is_empty() || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreatePrimaryKey {
                table_id,
                column_ids,
                name,
            })
        }
        Some("prepare")
            if matches!(version, Some(1 | 2))
                && operation.as_deref() == Some("constraints.create_check") =>
        {
            let has_name = version == Some(2);
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = if has_name {
                Some(args.remove("name").ok_or(ClientControlError::MissingKey)?)
            } else {
                None
            };
            let expression_bytes = decode_lower_hex_bytes(
                &args
                    .remove("expression_eir1")
                    .ok_or(ClientControlError::MissingKey)?,
                crate::expression::MAX_EXPRESSION_BYTES,
            )?;
            let expression = crate::expression::decode_expression(&expression_bytes)
                .map_err(|_| ClientControlError::InvalidPrepare)?;
            args.remove("dependency_column_ids")
                .ok_or(ClientControlError::MissingKey)?;
            let dependency_column_ids = check_dependency_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() || !crate::expression::is_row_local_expression(&expression) {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateCheckConstraint {
                table_id,
                name,
                expression,
                dependency_column_ids,
            })
        }
        Some("prepare")
            if version == Some(1)
                && matches!(
                    operation.as_deref(),
                    Some("policies.create" | "policies.replace")
                ) =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            let kind = match args
                .remove("kind")
                .ok_or(ClientControlError::MissingKey)?
                .as_str()
            {
                "row" => crate::storage_manager::PolicyKind::Row,
                "cell" => crate::storage_manager::PolicyKind::Cell,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            let policy_operation = match args
                .remove("policy_operation")
                .ok_or(ClientControlError::MissingKey)?
                .as_str()
            {
                "select" => crate::authorization::Operation::Select,
                "insert" => crate::authorization::Operation::Insert,
                "update" => crate::authorization::Operation::Update,
                "delete" => crate::authorization::Operation::Delete,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            let effect = match args
                .remove("effect")
                .ok_or(ClientControlError::MissingKey)?
                .as_str()
            {
                "allow" => crate::authorization::Effect::Allow,
                "restrict" => crate::authorization::Effect::Restrict,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            if args.remove("recipients").as_deref() != Some("<structured-recipients>") {
                return Err(ClientControlError::MissingKey);
            }
            let recipients = policy_recipients
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            if args.remove("target_column_ids").as_deref() != Some("<structured-ids>") {
                return Err(ClientControlError::MissingKey);
            }
            let target_column_ids = policy_target_column_ids
                .take()
                .ok_or(ClientControlError::MissingKey)?;
            let using_expression = decode_optional_policy_expression(
                args.remove("using_expression_eir1")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let with_check_expression = decode_optional_policy_expression(
                args.remove("with_check_expression_eir1")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let replaced_policy_id = if operation.as_deref() == Some("policies.replace") {
                Some(decode_lower_hex_id(
                    &args
                        .remove("replaced_policy_id")
                        .ok_or(ClientControlError::MissingKey)?,
                )?)
            } else {
                None
            };
            if !args.is_empty()
                || replaced_policy_id == Some([0; 16])
                || !is_canonical_admin_principal_name(&name)
                || recipients.is_empty()
                || recipients
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != recipients.len()
                || target_column_ids
                    .iter()
                    .copied()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != target_column_ids.len()
                || (kind == crate::storage_manager::PolicyKind::Row
                    && !target_column_ids.is_empty())
                || (kind == crate::storage_manager::PolicyKind::Cell
                    && (target_column_ids.is_empty()
                        || policy_operation == crate::authorization::Operation::Delete))
                || !match policy_operation {
                    crate::authorization::Operation::Select
                    | crate::authorization::Operation::Delete => {
                        using_expression.is_some() && with_check_expression.is_none()
                    }
                    crate::authorization::Operation::Insert => {
                        using_expression.is_none() && with_check_expression.is_some()
                    }
                    crate::authorization::Operation::Update => {
                        using_expression.is_some() && with_check_expression.is_some()
                    }
                    crate::authorization::Operation::Usage => false,
                }
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreatePolicy {
                replaced_policy_id,
                table_id,
                name,
                kind,
                operation: policy_operation,
                effect,
                recipients,
                target_column_ids,
                using_expression,
                with_check_expression,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("policies.drop") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let policy_id = decode_lower_hex_id(
                &args
                    .remove("policy_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropPolicy { policy_id })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("constraints.drop_check") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let constraint_id = decode_lower_hex_id(
                &args
                    .remove("constraint_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropCheckConstraint { constraint_id })
        }
        Some("prepare")
            if version == Some(1)
                && matches!(
                    operation.as_deref(),
                    Some("constraints.drop_primary_key" | "constraints.drop_unique")
                ) =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let constraint_id = decode_lower_hex_id(
                &args
                    .remove("constraint_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            if operation.as_deref() == Some("constraints.drop_primary_key") {
                Ok(ClientAdminCommand::PrepareDropPrimaryKey { constraint_id })
            } else {
                Ok(ClientAdminCommand::PrepareDropUniqueConstraint { constraint_id })
            }
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("tables.rename") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() || !is_canonical_admin_principal_name(&name) {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareRenameTable { table_id, name })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.set_owner") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let owner_id = decode_lower_hex_id(
                &args
                    .remove("owner_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareSetTableOwner { table_id, owner_id })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.rename_column") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let column_id = decode_lower_hex_id(
                &args
                    .remove("column_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() || !is_canonical_admin_principal_name(&name) {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareRenameColumn {
                table_id,
                column_id,
                name,
            })
        }
        Some("prepare")
            if version == Some(1)
                && operation.as_deref() == Some("tables.alter_column_nullability") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let column_id = decode_lower_hex_id(
                &args
                    .remove("column_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let nullable = match args.remove("nullable").as_deref() {
                Some("true") => true,
                Some("false") => false,
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareSetColumnNullable {
                table_id,
                column_id,
                nullable,
            })
        }
        Some("prepare")
            if version == Some(1)
                && operation.as_deref() == Some("tables.widen_varchar_column") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let column_id = decode_lower_hex_id(
                &args
                    .remove("column_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let max_chars_text = args
                .remove("max_chars")
                .ok_or(ClientControlError::MissingKey)?;
            let max_chars = max_chars_text
                .parse::<u32>()
                .ok()
                .filter(|value| *value > 0 && value.to_string() == max_chars_text)
                .ok_or(ClientControlError::InvalidPrepare)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareWidenVarcharColumn {
                table_id,
                column_id,
                max_chars,
            })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.set_column_default") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let column_id = decode_lower_hex_id(
                &args
                    .remove("column_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let scalar_type = decode_table_scalar_type(
                &args.remove("type").ok_or(ClientControlError::MissingKey)?,
            )?;
            if args.remove("default").as_deref() != Some("<structured-default>") || !args.is_empty()
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let (kind, value) = column_default.ok_or(ClientControlError::MissingKey)?;
            let default = decode_table_column_default(&kind, value, scalar_type)?;
            Ok(ClientAdminCommand::PrepareAlterColumnDefault {
                table_id,
                column_id,
                default: Some(default),
            })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.drop_column_default") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let column_id = decode_lower_hex_id(
                &args
                    .remove("column_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareAlterColumnDefault {
                table_id,
                column_id,
                default: None,
            })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.add_column") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if args.remove("column").as_deref() != Some("<structured-column>") || !args.is_empty() {
                return Err(ClientControlError::InvalidPrepare);
            }
            let column = table_column.ok_or(ClientControlError::MissingKey)?;
            if column.generated_uuid_identity
                || (!column.nullable && column.default.is_none())
                || matches!(
                    column.default,
                    Some(crate::storage_manager::TableColumnDefault::CurrentPrincipalId)
                )
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareAddColumn { table_id, column })
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("tables.drop_column") =>
        {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let table_id = decode_lower_hex_id(
                &args
                    .remove("table_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let column_id = decode_lower_hex_id(
                &args
                    .remove("column_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareDropColumn {
                table_id,
                column_id,
            })
        }
        Some("prepare")
            if matches!(version, Some(1..=4)) && operation.as_deref() == Some("tables.create") =>
        {
            if operation_id.is_some() || !(4..=14).contains(&seen.len()) {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let schema_id = decode_lower_hex_id(
                &args
                    .remove("schema_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
            let columns_value = args
                .remove("columns")
                .ok_or(ClientControlError::MissingKey)?;
            let columns = table_columns.ok_or(ClientControlError::MissingKey)?;
            let primary_key_marker = args.remove("primary_key_columns");
            if primary_key_marker.as_deref()
                != primary_key_columns.as_ref().map(|_| "<structured-names>")
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let unique_marker = args.remove("unique_constraints");
            if unique_marker.as_deref()
                != unique_constraints
                    .as_ref()
                    .map(|_| "<structured-name-arrays>")
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let unique_constraints = unique_constraints.unwrap_or_default();
            let check_marker = args.remove("check_expressions_eir1");
            if check_marker.as_deref()
                != check_expressions
                    .as_ref()
                    .map(|_| "<structured-check-expressions>")
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let check_expressions = check_expressions.unwrap_or_default();
            let names_marker = args.remove("check_constraint_names");
            if names_marker.as_deref()
                != check_constraint_names
                    .as_ref()
                    .map(|_| "<structured-check-names>")
                || (version.is_some_and(|version| version < 3) && check_constraint_names.is_some())
                || (version == Some(3) && check_constraint_names.is_none())
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let check_constraint_names = check_constraint_names
                .unwrap_or_default()
                .into_iter()
                .map(|name| if name.is_empty() { None } else { Some(name) })
                .collect::<Vec<_>>();
            if (!check_constraint_names.is_empty()
                && check_constraint_names.len() != check_expressions.len())
                || check_constraint_names
                    .iter()
                    .flatten()
                    .any(|name| !is_canonical_admin_principal_name(name))
                || check_constraint_names
                    .iter()
                    .flatten()
                    .collect::<std::collections::HashSet<_>>()
                    .len()
                    != check_constraint_names.iter().flatten().count()
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let source_marker = args.remove("foreign_key_source_columns");
            if source_marker.as_deref()
                != table_foreign_key_sources
                    .as_ref()
                    .map(|_| "<structured-foreign-key-sources>")
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let target_marker = args.remove("foreign_key_target_constraint_ids");
            if target_marker.as_deref()
                != table_foreign_key_targets
                    .as_ref()
                    .map(|_| "<structured-foreign-key-targets>")
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let source_columns = table_foreign_key_sources.unwrap_or_default();
            let target_constraint_ids = table_foreign_key_targets.unwrap_or_default();
            let target_columns_marker = args.remove("foreign_key_target_columns");
            if target_columns_marker.as_deref()
                != table_foreign_key_target_columns
                    .as_ref()
                    .map(|_| "<structured-foreign-key-target-columns>")
                || (version == Some(1) && table_foreign_key_target_columns.is_some())
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let target_columns = table_foreign_key_target_columns
                .unwrap_or_else(|| vec![Vec::new(); target_constraint_ids.len()]);
            if source_columns.len() != target_constraint_ids.len()
                || source_columns.len() != target_columns.len()
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let primary_key_name_field = args.remove("primary_key_name");
            let unique_names_marker = args.remove("unique_constraint_names");
            let foreign_names_marker = args.remove("foreign_key_names");
            if version == Some(4) {
                if primary_key_name_field.is_none()
                    || unique_names_marker.as_deref() != Some("<structured-unique-names>")
                    || foreign_names_marker.as_deref() != Some("<structured-foreign-key-names>")
                    || unique_constraint_names.is_none()
                    || foreign_key_names.is_none()
                {
                    return Err(ClientControlError::InvalidPrepare);
                }
            } else if primary_key_name_field.is_some()
                || unique_names_marker.is_some()
                || foreign_names_marker.is_some()
                || unique_constraint_names.is_some()
                || foreign_key_names.is_some()
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let primary_key_name = primary_key_name_field.filter(|name| !name.is_empty());
            let unique_constraint_names = unique_constraint_names
                .unwrap_or_default()
                .into_iter()
                .map(|name| if name.is_empty() { None } else { Some(name) })
                .collect::<Vec<_>>();
            let foreign_key_names = foreign_key_names
                .unwrap_or_default()
                .into_iter()
                .map(|name| if name.is_empty() { None } else { Some(name) })
                .collect::<Vec<_>>();
            let named_constraint_names = primary_key_name
                .iter()
                .chain(unique_constraint_names.iter().flatten())
                .chain(check_constraint_names.iter().flatten())
                .chain(foreign_key_names.iter().flatten())
                .collect::<std::collections::HashSet<_>>();
            if primary_key_name
                .as_ref()
                .is_some_and(|_| primary_key_columns.is_none())
                || (!unique_constraint_names.is_empty()
                    && unique_constraint_names.len() != unique_constraints.len())
                || (!foreign_key_names.is_empty()
                    && foreign_key_names.len() != target_columns.len())
                || unique_constraint_names
                    .iter()
                    .flatten()
                    .chain(foreign_key_names.iter().flatten())
                    .chain(check_constraint_names.iter().flatten())
                    .chain(primary_key_name.iter())
                    .any(|name| !is_canonical_admin_principal_name(name))
                || named_constraint_names.len()
                    != usize::from(primary_key_name.is_some())
                        + unique_constraint_names.iter().flatten().count()
                        + check_constraint_names.iter().flatten().count()
                        + foreign_key_names.iter().flatten().count()
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            let foreign_keys = source_columns
                .into_iter()
                .zip(target_constraint_ids.into_iter().zip(target_columns))
                .map(|(source_columns, (target_constraint_id, target_columns))| {
                    let target_columns = if target_columns.is_empty() {
                        None
                    } else {
                        Some(target_columns)
                    };
                    if source_columns.is_empty()
                        || source_columns.len() > columns.len()
                        || (target_constraint_id == [0; 16]) != target_columns.is_some()
                        || target_columns.as_ref().is_some_and(|names| {
                            names.len() > columns.len()
                                || names.iter().collect::<std::collections::HashSet<_>>().len()
                                    != names.len()
                                || names
                                    .iter()
                                    .any(|name| !columns.iter().any(|column| column.name == *name))
                        })
                        || source_columns
                            .iter()
                            .collect::<std::collections::HashSet<_>>()
                            .len()
                            != source_columns.len()
                        || source_columns
                            .iter()
                            .any(|name| !columns.iter().any(|column| column.name == *name))
                    {
                        return Err(ClientControlError::InvalidPrepare);
                    }
                    Ok(crate::storage_manager::CreateTableForeignKeyDefinition {
                        source_columns,
                        target_constraint_id,
                        target_columns,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            if columns_value != "<structured-columns>"
                || !is_canonical_admin_principal_name(&name)
                || columns.is_empty()
                || columns.len() > crate::tuple::MAX_COLUMNS
                || (primary_key_columns.is_some()
                    && (primary_key_columns.as_ref().is_some_and(Vec::is_empty)
                        || primary_key_columns.as_ref().is_some_and(|names| {
                            names.len() > columns.len()
                                || names.iter().collect::<std::collections::HashSet<_>>().len()
                                    != names.len()
                                || names.iter().any(|name| {
                                    !columns
                                        .iter()
                                        .any(|column| column.name == *name && !column.nullable)
                                })
                        })))
                || !args.is_empty()
                || unique_constraints.len() > crate::tuple::MAX_COLUMNS
                || unique_constraints.iter().any(|names| {
                    names.is_empty()
                        || names.len() > columns.len()
                        || names.iter().collect::<std::collections::HashSet<_>>().len()
                            != names.len()
                        || names
                            .iter()
                            .any(|name| !columns.iter().any(|column| column.name == *name))
                })
                || unique_constraints
                    .iter()
                    .enumerate()
                    .any(|(index, names)| unique_constraints[..index].contains(names))
                || check_expressions.len() > crate::tuple::MAX_COLUMNS
                || foreign_keys.len() > crate::tuple::MAX_COLUMNS
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareCreateTable {
                schema_id,
                name,
                columns,
                primary_key_columns,
                primary_key_name,
                unique_constraints,
                unique_constraint_names,
                check_expressions,
                check_constraint_names,
                foreign_keys,
                foreign_key_names,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("users.rename") => {
            decode_identity_rename_prepare(arguments, operation_id, seen.len(), "user_id")
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("roles.rename") => {
            decode_identity_rename_prepare(arguments, operation_id, seen.len(), "role_id")
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("users.drop") => {
            decode_identity_drop_prepare(arguments, operation_id, seen.len(), "user_id")
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("roles.drop") => {
            decode_identity_drop_prepare(arguments, operation_id, seen.len(), "role_id")
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("users.login") => {
            decode_user_login_prepare(arguments, operation_id, seen.len())
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("roles.grant") => {
            decode_role_membership_prepare(arguments, operation_id, seen.len(), true)
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("roles.revoke") => {
            decode_role_membership_prepare(arguments, operation_id, seen.len(), false)
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("privileges.grant") =>
        {
            decode_system_privilege_prepare(arguments, operation_id, seen.len(), true)
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("privileges.revoke") =>
        {
            decode_system_privilege_prepare(arguments, operation_id, seen.len(), false)
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("objects.grant") => {
            decode_object_grant_prepare(arguments, operation_id, seen.len(), true)
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("objects.revoke") => {
            decode_object_grant_prepare(arguments, operation_id, seen.len(), false)
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("objects.grant_batch") =>
        {
            decode_object_grant_prepare(arguments, operation_id, seen.len(), true)
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("objects.revoke_batch") =>
        {
            decode_object_grant_prepare(arguments, operation_id, seen.len(), false)
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("objects.grant_entries") =>
        {
            decode_object_grant_prepare(arguments, operation_id, seen.len(), true)
        }
        Some("prepare")
            if version == Some(1) && operation.as_deref() == Some("objects.revoke_entries") =>
        {
            decode_object_grant_prepare(arguments, operation_id, seen.len(), false)
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("tokens.revoke") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let token_id = decode_lower_hex_id(
                &args
                    .remove("token_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            Ok(ClientAdminCommand::PrepareRevoke { token_id })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("tokens.issue") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let user_id = decode_lower_hex_id(
                &args
                    .remove("user_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let label = args.remove("label").ok_or(ClientControlError::MissingKey)?;
            let expires_at_us = args
                .remove("expires_at_us")
                .ok_or(ClientControlError::MissingKey)?;
            if !args.is_empty() {
                return Err(ClientControlError::UnknownKey);
            }
            if label.is_empty() || label.len() > 128 {
                return Err(ClientControlError::InvalidPrepare);
            }
            let expires_at_us = parse_canonical_positive_i64(&expires_at_us)
                .ok_or(ClientControlError::InvalidPrepare)?;
            Ok(ClientAdminCommand::PrepareIssue {
                user_id,
                label,
                expires_at_us,
            })
        }
        Some("prepare") if version == Some(1) && operation.as_deref() == Some("tokens.rotate") => {
            if operation_id.is_some() || seen.len() != 4 {
                return Err(ClientControlError::UnknownKey);
            }
            let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
            let old_token_id = decode_lower_hex_id(
                &args
                    .remove("old_token_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            let label = args.remove("label").ok_or(ClientControlError::MissingKey)?;
            let expires_at_us = parse_canonical_positive_i64(
                &args
                    .remove("expires_at_us")
                    .ok_or(ClientControlError::MissingKey)?,
            )
            .ok_or(ClientControlError::InvalidPrepare)?;
            if !args.is_empty() || label.is_empty() || label.len() > 128 {
                return Err(ClientControlError::InvalidPrepare);
            }
            Ok(ClientAdminCommand::PrepareRotateToken {
                old_token_id,
                label,
                expires_at_us,
            })
        }
        Some("start") if version.is_none() && operation.is_none() => {
            if arguments.is_some() || seen.len() != 2 {
                return Err(ClientControlError::UnknownKey);
            }
            let operation_id =
                decode_lower_hex_id(&operation_id.ok_or(ClientControlError::MissingKey)?)?;
            Ok(ClientAdminCommand::Start { operation_id })
        }
        _ => Err(ClientControlError::UnsupportedAdminOperation),
    }
}

pub(super) fn decode_role_membership_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
    grant: bool,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let member_id = decode_lower_hex_id(
        &args
            .remove("member_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    let role_id = decode_lower_hex_id(
        &args
            .remove("role_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    if !args.is_empty() || member_id == role_id {
        return Err(ClientControlError::InvalidPrepare);
    }
    Ok(if grant {
        ClientAdminCommand::PrepareGrantRole { member_id, role_id }
    } else {
        ClientAdminCommand::PrepareRevokeRole { member_id, role_id }
    })
}

pub(super) fn decode_system_privilege_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
    grant: bool,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let grantee_id = decode_lower_hex_id(
        &args
            .remove("grantee_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    let privilege = match args
        .remove("privilege")
        .ok_or(ClientControlError::MissingKey)?
        .as_str()
    {
        "manage_users" => crate::storage_manager::AdministrativeSystemPrivilege::ManageUsers,
        "manage_roles" => crate::storage_manager::AdministrativeSystemPrivilege::ManageRoles,
        "create_schema" => crate::storage_manager::AdministrativeSystemPrivilege::CreateSchema,
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    if !args.is_empty() {
        return Err(ClientControlError::UnknownKey);
    }
    Ok(ClientAdminCommand::PrepareSystemPrivilege {
        grantee_id,
        privilege,
        grant,
    })
}

pub(super) fn decode_object_grant_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
    grant: bool,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let grantee_id = decode_lower_hex_id(
        &args
            .remove("grantee_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    let object_id = decode_lower_hex_id(
        &args
            .remove("object_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    if let Some(encoded_entries) = args.remove("entries") {
        if !args.is_empty() || encoded_entries.is_empty() {
            return Err(ClientControlError::InvalidPrepare);
        }
        let mut entries = Vec::new();
        for encoded in encoded_entries.split(';') {
            let (encoded_privilege, encoded_target) = encoded
                .split_once('@')
                .ok_or(ClientControlError::InvalidPrepare)?;
            let privilege = decode_object_privilege(encoded_privilege)?;
            let (scope, ids) = encoded_target
                .split_once(':')
                .ok_or(ClientControlError::InvalidPrepare)?;
            let target = match scope {
                "object" if decode_lower_hex_id(ids)? == object_id => {
                    crate::storage_manager::ObjectGrantTarget::Object
                }
                "column" => {
                    crate::storage_manager::ObjectGrantTarget::Column(decode_lower_hex_id(ids)?)
                }
                "columns" => {
                    let mut values = Vec::new();
                    for value in ids.split('+') {
                        values.push(decode_lower_hex_id(value)?);
                    }
                    if !(2..=64).contains(&values.len()) {
                        return Err(ClientControlError::InvalidPrepare);
                    }
                    let mut seen = HashSet::new();
                    if values.iter().any(|value| !seen.insert(*value)) {
                        return Err(ClientControlError::InvalidPrepare);
                    }
                    crate::storage_manager::ObjectGrantTarget::Columns(values)
                }
                "all_columns" if ids.is_empty() => {
                    crate::storage_manager::ObjectGrantTarget::AllColumns
                }
                _ => return Err(ClientControlError::InvalidPrepare),
            };
            if entries
                .iter()
                .any(|entry: &crate::storage_manager::ObjectGrantEntry| {
                    entry.privilege == privilege && entry.target == target
                })
            {
                return Err(ClientControlError::InvalidPrepare);
            }
            entries.push(crate::storage_manager::ObjectGrantEntry { privilege, target });
            if entries.len() > 64 {
                return Err(ClientControlError::InvalidPrepare);
            }
        }
        return Ok(ClientAdminCommand::PrepareObjectGrant {
            grantee_id,
            object_id,
            privileges: Vec::new(),
            target: crate::storage_manager::ObjectGrantTarget::Object,
            entries: Some(entries),
            grant,
        });
    }
    let encoded_privileges = match (args.remove("privilege"), args.remove("privileges")) {
        (Some(single), None) => single,
        (None, Some(multiple)) => multiple,
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    let mut privileges = Vec::new();
    for encoded in encoded_privileges.split(',') {
        let privilege = decode_object_privilege(encoded)?;
        if privileges.contains(&privilege) {
            return Err(ClientControlError::InvalidPrepare);
        }
        privileges.push(privilege);
    }
    if privileges.is_empty() || privileges.len() > 11 {
        return Err(ClientControlError::InvalidPrepare);
    }
    let target_scope = args
        .remove("target_scope")
        .ok_or(ClientControlError::MissingKey)?;
    let target = match target_scope.as_str() {
        "object" => {
            let target_id = decode_lower_hex_id(
                &args
                    .remove("target_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            if target_id != object_id {
                return Err(ClientControlError::InvalidPrepare);
            }
            crate::storage_manager::ObjectGrantTarget::Object
        }
        "column" => {
            let target_id = decode_lower_hex_id(
                &args
                    .remove("target_id")
                    .ok_or(ClientControlError::MissingKey)?,
            )?;
            crate::storage_manager::ObjectGrantTarget::Column(target_id)
        }
        "columns" => {
            let encoded_ids = args
                .remove("target_ids")
                .ok_or(ClientControlError::MissingKey)?;
            let encoded_ids = encoded_ids.split(',').collect::<Vec<_>>();
            if !(2..=64).contains(&encoded_ids.len()) {
                return Err(ClientControlError::InvalidPrepare);
            }
            let mut target_ids = Vec::with_capacity(encoded_ids.len());
            let mut seen_ids = HashSet::with_capacity(encoded_ids.len());
            for encoded_id in encoded_ids {
                let target_id = decode_lower_hex_id(encoded_id)?;
                if !seen_ids.insert(target_id) {
                    return Err(ClientControlError::InvalidPrepare);
                }
                target_ids.push(target_id);
            }
            crate::storage_manager::ObjectGrantTarget::Columns(target_ids)
        }
        "all_columns" => crate::storage_manager::ObjectGrantTarget::AllColumns,
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    if !args.is_empty() {
        return Err(ClientControlError::UnknownKey);
    }
    Ok(ClientAdminCommand::PrepareObjectGrant {
        grantee_id,
        object_id,
        privileges,
        target,
        entries: None,
        grant,
    })
}

pub(super) fn decode_object_privilege(
    encoded: &str,
) -> Result<crate::storage_manager::ObjectGrantPrivilege, ClientControlError> {
    match encoded {
        "usage" => Ok(crate::storage_manager::ObjectGrantPrivilege::Usage),
        "select" => Ok(crate::storage_manager::ObjectGrantPrivilege::Select),
        "insert" => Ok(crate::storage_manager::ObjectGrantPrivilege::Insert),
        "update" => Ok(crate::storage_manager::ObjectGrantPrivilege::Update),
        "delete" => Ok(crate::storage_manager::ObjectGrantPrivilege::Delete),
        "references" => Ok(crate::storage_manager::ObjectGrantPrivilege::References),
        "create" => Ok(crate::storage_manager::ObjectGrantPrivilege::Create),
        "alter" => Ok(crate::storage_manager::ObjectGrantPrivilege::Alter),
        "index" => Ok(crate::storage_manager::ObjectGrantPrivilege::Index),
        "manage_policies" => Ok(crate::storage_manager::ObjectGrantPrivilege::ManagePolicies),
        "manage_grants" => Ok(crate::storage_manager::ObjectGrantPrivilege::ManageGrants),
        _ => Err(ClientControlError::InvalidPrepare),
    }
}

pub(super) fn decode_optional_policy_expression(
    value: String,
) -> Result<Option<crate::authorization::Expr>, ClientControlError> {
    if value == "<null>" {
        return Ok(None);
    }
    let bytes = decode_lower_hex_bytes(&value, crate::expression::MAX_EXPRESSION_BYTES)?;
    crate::expression::decode_expression(&bytes)
        .map(Some)
        .map_err(|_| ClientControlError::InvalidPrepare)
}

pub(super) fn decode_identity_rename_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
    id_field: &str,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let identity_id = decode_lower_hex_id(
        &args
            .remove(id_field)
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    let name = args.remove("name").ok_or(ClientControlError::MissingKey)?;
    if !is_canonical_admin_principal_name(&name) || !args.is_empty() {
        return Err(ClientControlError::InvalidPrepare);
    }
    Ok(ClientAdminCommand::PrepareRenameIdentity { identity_id, name })
}

pub(super) fn decode_user_login_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let user_id = decode_lower_hex_id(
        &args
            .remove("user_id")
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    let enabled = match args
        .remove("enabled")
        .ok_or(ClientControlError::MissingKey)?
        .as_str()
    {
        "true" => true,
        "false" => false,
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    if !args.is_empty() {
        return Err(ClientControlError::UnknownKey);
    }
    Ok(ClientAdminCommand::PrepareSetUserLogin { user_id, enabled })
}

pub(super) fn decode_identity_drop_prepare(
    arguments: Option<HashMap<String, String>>,
    operation_id: Option<String>,
    seen_len: usize,
    id_field: &str,
) -> Result<ClientAdminCommand, ClientControlError> {
    if operation_id.is_some() || seen_len != 4 {
        return Err(ClientControlError::UnknownKey);
    }
    let mut args = arguments.ok_or(ClientControlError::MissingKey)?;
    let identity_id = decode_lower_hex_id(
        &args
            .remove(id_field)
            .ok_or(ClientControlError::MissingKey)?,
    )?;
    if !args.is_empty() {
        return Err(ClientControlError::UnknownKey);
    }
    Ok(ClientAdminCommand::PrepareDropIdentity { identity_id })
}

pub(super) fn is_canonical_admin_principal_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 128
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
}

pub(super) fn parse_canonical_positive_i64(value: &str) -> Option<i64> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse::<i64>().ok().filter(|value| *value > 0)
}
