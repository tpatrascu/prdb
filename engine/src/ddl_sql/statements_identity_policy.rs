use super::ast::*;
use super::lexer::*;
use super::parser::*;

/// Parse the bounded administrative `CREATE USER` or `CREATE ROLE` form.
pub fn parse_create_identity(sql: &str) -> Result<CreateIdentitySql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("create")?;
    let kind = if parser.peek_word("user") {
        parser.word("user")?;
        CreateIdentityKind::User
    } else {
        parser.word("role")?;
        CreateIdentityKind::Role
    };
    let if_not_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("not")?;
        parser.word("exists")?;
        true
    } else {
        false
    };
    let name = parser.identifier()?;
    let login_enabled = if kind == CreateIdentityKind::User {
        if parser.peek_word("login") {
            parser.word("login")?;
            true
        } else {
            parser.word("nologin")?;
            false
        }
    } else {
        false
    };
    finish_statement(&mut parser)?;
    Ok(CreateIdentitySql {
        kind,
        if_not_exists,
        name,
        login_enabled,
    })
}

/// Parse `ALTER USER|ROLE old_name RENAME TO new_name`.
pub fn parse_rename_identity(sql: &str) -> Result<RenameIdentitySql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    let kind = if parser.peek_word("user") {
        parser.word("user")?;
        CreateIdentityKind::User
    } else {
        parser.word("role")?;
        CreateIdentityKind::Role
    };
    let old_name = parser.identifier()?;
    parser.word("rename")?;
    parser.word("to")?;
    let new_name = parser.identifier()?;
    finish_statement(&mut parser)?;
    Ok(RenameIdentitySql {
        kind,
        old_name,
        new_name,
    })
}

/// Parse `DROP USER|ROLE [IF EXISTS] name RESTRICT`.
pub fn parse_drop_identity(sql: &str) -> Result<DropIdentitySql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("drop")?;
    let kind = if parser.peek_word("user") {
        parser.word("user")?;
        CreateIdentityKind::User
    } else {
        parser.word("role")?;
        CreateIdentityKind::Role
    };
    let if_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("exists")?;
        true
    } else {
        false
    };
    let name = parser.identifier()?;
    parser.word("restrict")?;
    finish_statement(&mut parser)?;
    Ok(DropIdentitySql {
        kind,
        name,
        if_exists,
    })
}

/// Parse `ALTER USER name LOGIN|NOLOGIN`.
pub fn parse_set_user_login(sql: &str) -> Result<SetUserLoginSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("user")?;
    let name = parser.identifier()?;
    let enabled = if parser.peek_word("login") {
        parser.word("login")?;
        true
    } else {
        parser.word("nologin")?;
        false
    };
    finish_statement(&mut parser)?;
    Ok(SetUserLoginSql { name, enabled })
}

/// Parse `GRANT role TO USER|ROLE member` and its REVOKE counterpart.
pub fn parse_role_membership(sql: &str) -> Result<RoleMembershipSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    let grant = if parser.peek_word("grant") {
        parser.word("grant")?;
        true
    } else {
        parser.word("revoke")?;
        false
    };
    let role_name = parser.identifier()?;
    if grant {
        parser.word("to")?;
    } else {
        parser.word("from")?;
    }
    let member_is_role = if parser.peek_word("user") {
        parser.word("user")?;
        false
    } else {
        parser.word("role")?;
        true
    };
    let member_name = parser.identifier()?;
    finish_statement(&mut parser)?;
    Ok(RoleMembershipSql {
        grant,
        role_name,
        member_name,
        member_is_role,
    })
}

/// Parse a database-scoped system privilege grant or revoke.
///
/// Both the explicit `SYSTEM PRIVILEGE` form and the concise `ON DATABASE`
/// form are accepted for compatibility with the permission DDL syntax.
pub fn parse_system_privilege(sql: &str) -> Result<SystemPrivilegeSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    let grant = if parser.peek_word("grant") {
        parser.word("grant")?;
        true
    } else {
        parser.word("revoke")?;
        false
    };
    let explicit_system_privilege = parser.peek_word("system");
    let privilege = if explicit_system_privilege {
        parser.word("system")?;
        parser.word("privilege")?;
        parser.identifier()?
    } else {
        parser.identifier()?
    };
    if !matches!(
        privilege.as_str(),
        "manage_users" | "manage_roles" | "create_schema"
    ) {
        return Err(DdlSqlError::InvalidStatement);
    }
    if !explicit_system_privilege {
        parser.word("on")?;
        parser.word("database")?;
    }
    if grant {
        parser.word("to")?;
    } else {
        parser.word("from")?;
    }
    let grantee_is_role = if parser.peek_word("user") {
        parser.word("user")?;
        false
    } else {
        parser.word("role")?;
        true
    };
    let grantee_name = parser.identifier()?;
    finish_statement(&mut parser)?;
    Ok(SystemPrivilegeSql {
        grant,
        privilege,
        grantee_name,
        grantee_is_role,
    })
}

/// Parse a bounded object-scoped privilege grant/revoke on a schema or table.
pub fn parse_object_privilege(sql: &str) -> Result<ObjectPrivilegeSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    let grant = if parser.peek_word("grant") {
        parser.word("grant")?;
        true
    } else {
        parser.word("revoke")?;
        false
    };
    let mut privileges = vec![parser.identifier()?];
    while parser.peek() == Some(&Token::Comma) {
        parser.take(Token::Comma)?;
        let privilege = parser.identifier()?;
        if privileges.len() == 11 || privileges.contains(&privilege) {
            return Err(DdlSqlError::InvalidStatement);
        }
        privileges.push(privilege);
    }
    if privileges.iter().any(|privilege| {
        !matches!(
            privilege.as_str(),
            "usage"
                | "select"
                | "insert"
                | "update"
                | "delete"
                | "references"
                | "create"
                | "alter"
                | "index"
                | "manage_policies"
                | "manage_grants"
        )
    }) {
        return Err(DdlSqlError::InvalidStatement);
    }
    let mut column_names = Vec::new();
    if parser.peek() == Some(&Token::LeftParen) {
        parser.take(Token::LeftParen)?;
        loop {
            let column = parser.identifier()?;
            if column_names.contains(&column) || column_names.len() == 64 {
                return Err(DdlSqlError::InvalidStatement);
            }
            column_names.push(column);
            if parser.peek() != Some(&Token::Comma) {
                break;
            }
            parser.take(Token::Comma)?;
        }
        parser.take(Token::RightParen)?;
    }
    if !column_names.is_empty() {
        let column_privilege_count = privileges
            .iter()
            .filter(|privilege| {
                matches!(
                    privilege.as_str(),
                    "select" | "insert" | "update" | "references"
                )
            })
            .count();
        // A column list can scope one data privilege while other table
        // privileges in the same statement remain object-scoped.
        if column_privilege_count != 1 {
            return Err(DdlSqlError::InvalidStatement);
        }
    }
    parser.word("on")?;
    let is_table = if parser.peek_word("schema") {
        parser.word("schema")?;
        false
    } else {
        parser.word("table")?;
        true
    };
    for privilege in &privileges {
        if (is_table && matches!(privilege.as_str(), "usage" | "create"))
            || (!is_table
                && matches!(
                    privilege.as_str(),
                    "select"
                        | "insert"
                        | "update"
                        | "delete"
                        | "references"
                        | "index"
                        | "manage_policies"
                        | "manage_grants"
                ))
        {
            return Err(DdlSqlError::InvalidStatement);
        }
    }
    if !column_names.is_empty() && !is_table {
        return Err(DdlSqlError::InvalidStatement);
    }
    let schema_name = parser.identifier()?;
    let table_name = if is_table {
        parser.take(Token::Dot)?;
        Some(parser.identifier()?)
    } else {
        None
    };
    if grant {
        parser.word("to")?;
    } else {
        parser.word("from")?;
    }
    let grantee_is_role = if parser.peek_word("user") {
        parser.word("user")?;
        false
    } else {
        parser.word("role")?;
        true
    };
    let grantee_name = parser.identifier()?;
    finish_statement(&mut parser)?;
    Ok(ObjectPrivilegeSql {
        grant,
        privileges,
        schema_name,
        table_name,
        column_names,
        grantee_name,
        grantee_is_role,
    })
}

/// Parse one bounded CREATE ROW/CELL POLICY statement.
pub fn parse_create_policy(sql: &str) -> Result<CreatePolicySql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("create")?;
    let replace_existing = if parser.peek_word("or") {
        parser.word("or")?;
        parser.word("replace")?;
        true
    } else {
        false
    };
    let is_cell = if parser.peek_word("cell") {
        parser.word("cell")?;
        true
    } else {
        parser.word("row")?;
        false
    };
    parser.word("policy")?;
    let name = parser.identifier()?;
    parser.word("on")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    let columns = if is_cell && parser.peek() == Some(&Token::LeftParen) {
        parse_column_name_list(&mut parser)?
    } else {
        Vec::new()
    };
    parser.word("for")?;
    let operation = ["select", "insert", "update", "delete"]
        .into_iter()
        .find(|operation| parser.peek_word(operation))
        .ok_or(DdlSqlError::InvalidStatement)?;
    parser.position += 1;
    parser.word("to")?;
    let mut recipients = Vec::new();
    loop {
        let is_role = if parser.peek_word("user") {
            parser.word("user")?;
            false
        } else {
            parser.word("role")?;
            true
        };
        recipients.push(PolicyRecipientSql {
            name: parser.identifier()?,
            is_role,
        });
        if parser.peek() != Some(&Token::Comma) {
            break;
        }
        parser.position += 1;
    }
    parser.word("as")?;
    let effect = if parser.peek_word("allow") {
        parser.word("allow")?;
        "allow"
    } else {
        parser.word("restrict")?;
        "restrict"
    };
    let mut using_expression = None;
    let mut with_check_expression = None;
    while parser.peek_word("using") || parser.peek_word("with") {
        if parser.peek_word("using") {
            if using_expression.is_some() {
                return Err(DdlSqlError::InvalidStatement);
            }
            parser.word("using")?;
            parser.take(Token::LeftParen)?;
            using_expression = Some(parser.check_or()?);
            parser.take(Token::RightParen)?;
        } else {
            if with_check_expression.is_some() {
                return Err(DdlSqlError::InvalidStatement);
            }
            parser.word("with")?;
            parser.word("check")?;
            parser.take(Token::LeftParen)?;
            with_check_expression = Some(parser.check_or()?);
            parser.take(Token::RightParen)?;
        }
    }
    finish_statement(&mut parser)?;
    let required_clauses = match operation {
        "select" | "delete" => using_expression.is_some() && with_check_expression.is_none(),
        "insert" => using_expression.is_none() && with_check_expression.is_some(),
        "update" => using_expression.is_some() && with_check_expression.is_some(),
        _ => false,
    };
    if recipients.is_empty()
        || recipients.len() > crate::authorization::MAX_POLICY_SUBJECTS
        || recipients
            .iter()
            .map(|recipient| (recipient.is_role, &recipient.name))
            .collect::<std::collections::HashSet<_>>()
            .len()
            != recipients.len()
        || (is_cell && (columns.is_empty() || has_duplicates(&columns)))
        || (!is_cell && !columns.is_empty())
        || (is_cell && operation == "delete")
        || !required_clauses
    {
        return Err(DdlSqlError::InvalidStatement);
    }
    Ok(CreatePolicySql {
        replace_existing,
        schema,
        table,
        name,
        is_cell,
        columns,
        operation: operation.to_owned(),
        recipients,
        effect: effect.to_owned(),
        using_expression,
        with_check_expression,
    })
}

pub fn parse_drop_policy(sql: &str) -> Result<DropPolicySql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("drop")?;
    let is_cell = if parser.peek_word("cell") {
        parser.word("cell")?;
        true
    } else {
        parser.word("row")?;
        false
    };
    parser.word("policy")?;
    let if_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("exists")?;
        true
    } else {
        false
    };
    let name = parser.identifier()?;
    parser.word("on")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("restrict")?;
    finish_statement(&mut parser)?;
    Ok(DropPolicySql {
        schema,
        table,
        name,
        is_cell,
        if_exists,
    })
}
