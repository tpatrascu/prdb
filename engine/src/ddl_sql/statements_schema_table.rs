use super::MAX_DDL_BYTES;
use super::ast::*;
use super::binding::*;
use super::lexer::*;
use super::parser::*;
use crate::{
    client_submit::{AdminColumnDefault, AdminColumnDefinition},
    tuple::ScalarType,
};

pub fn parse_create_table(sql: &str) -> Result<CreateTableSql, DdlSqlError> {
    if sql.is_empty() || sql.len() > MAX_DDL_BYTES || sql.as_bytes().contains(&0) {
        return Err(DdlSqlError::InvalidStatement);
    }
    let tokens = tokenize(sql)?;
    let mut parser = Parser {
        tokens,
        position: 0,
    };
    parser.word("create")?;
    parser.word("table")?;
    let if_not_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("not")?;
        parser.word("exists")?;
        true
    } else {
        false
    };
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.take(Token::LeftParen)?;
    let mut columns = Vec::new();
    let mut primary_key_columns = None;
    let mut primary_key_name = None;
    let mut unique_constraints = Vec::new();
    let mut unique_constraint_names = Vec::new();
    let mut check_constraints = Vec::new();
    let mut check_constraint_names = Vec::new();
    let mut foreign_keys = Vec::new();
    let mut saw_table_constraint = false;
    loop {
        let constraint_name = if parser.peek_word("constraint") {
            parser.word("constraint")?;
            Some(parser.identifier()?)
        } else {
            None
        };
        if parser.peek_word("primary")
            || parser.peek_word("unique")
            || parser.peek_word("check")
            || parser.peek_word("foreign")
        {
            saw_table_constraint = true;
            if parser.peek_word("check") {
                if constraint_name.is_some() && check_constraints.len() >= crate::tuple::MAX_COLUMNS
                {
                    return Err(DdlSqlError::InvalidStatement);
                }
                let expression = parser.check_constraint_expression()?;
                check_constraints.push(expression);
                check_constraint_names.push(constraint_name);
            } else if parser.peek_word("foreign") {
                if foreign_keys.len() >= crate::tuple::MAX_COLUMNS {
                    return Err(DdlSqlError::InvalidStatement);
                }
                parser.word("foreign")?;
                parser.word("key")?;
                let columns = parse_column_name_list(&mut parser)?;
                parser.word("references")?;
                let referenced_schema = parser.identifier()?;
                parser.take(Token::Dot)?;
                let referenced_table = parser.identifier()?;
                let referenced_columns = parse_column_name_list(&mut parser)?;
                if columns.len() > crate::tuple::MAX_COLUMNS
                    || columns.len() != referenced_columns.len()
                    || has_duplicates(&columns)
                    || has_duplicates(&referenced_columns)
                {
                    return Err(DdlSqlError::InvalidStatement);
                }
                foreign_keys.push(CreateTableForeignKeySql {
                    name: constraint_name.clone(),
                    columns,
                    referenced_schema,
                    referenced_table,
                    referenced_columns,
                });
            } else {
                let kind = if parser.peek_word("primary") {
                    parser.word("primary")?;
                    parser.word("key")?;
                    2
                } else {
                    parser.word("unique")?;
                    3
                };
                parser.take(Token::LeftParen)?;
                let mut names = vec![parser.identifier()?];
                while parser.peek() == Some(&Token::Comma) {
                    parser.position += 1;
                    names.push(parser.identifier()?);
                }
                parser.take(Token::RightParen)?;
                if names.len() > crate::tuple::MAX_COLUMNS
                    || names.iter().collect::<std::collections::HashSet<_>>().len() != names.len()
                {
                    return Err(DdlSqlError::InvalidStatement);
                }
                if kind == 2 {
                    if primary_key_columns.replace(names).is_some() {
                        return Err(DdlSqlError::InvalidStatement);
                    }
                    primary_key_name = constraint_name;
                } else {
                    if unique_constraints.len() >= crate::tuple::MAX_COLUMNS
                        || unique_constraints.contains(&names)
                    {
                        return Err(DdlSqlError::InvalidStatement);
                    }
                    unique_constraints.push(names);
                    unique_constraint_names.push(constraint_name);
                }
            }
            if parser.peek() == Some(&Token::Comma) {
                parser.position += 1;
                continue;
            }
            break;
        }
        if saw_table_constraint {
            return Err(DdlSqlError::InvalidStatement);
        }
        if columns.len() == 4096 {
            return Err(DdlSqlError::InvalidStatement);
        }
        let name = parser.identifier()?;
        let scalar_type = parser.scalar_type()?;
        let generated_uuid_identity = if parser.peek_word("generated") {
            parser.word("generated")?;
            parser.word("always")?;
            parser.word("as")?;
            parser.word("identity")?;
            if scalar_type != ScalarType::Uuid {
                return Err(DdlSqlError::InvalidStatement);
            }
            true
        } else {
            false
        };
        let mut nullable = true;
        let mut default = None;
        let mut unique = false;
        let mut unique_name = None;
        let mut primary_key = false;
        let mut primary_key_column_name = None;
        loop {
            if parser.peek_word("not") && nullable {
                parser.word("not")?;
                parser.word("null")?;
                nullable = false;
            } else if parser.peek_word("default") && default.is_none() {
                parser.word("default")?;
                default = Some(parser.default_value(scalar_type)?);
            } else if parser.peek_word("unique") && !unique {
                parser.word("unique")?;
                unique = true;
            } else if parser.peek_word("constraint") {
                parser.word("constraint")?;
                let constraint_name = parser.identifier()?;
                if parser.peek_word("check") {
                    if check_constraints.len() >= crate::tuple::MAX_COLUMNS {
                        return Err(DdlSqlError::InvalidStatement);
                    }
                    let expression = parser.check_constraint_expression()?;
                    validate_column_check_expression(&name, &expression)?;
                    check_constraints.push(expression);
                    check_constraint_names.push(Some(constraint_name));
                } else if parser.peek_word("primary") && !primary_key {
                    parser.word("primary")?;
                    parser.word("key")?;
                    primary_key = true;
                    primary_key_column_name = Some(constraint_name);
                } else if parser.peek_word("unique") && !unique {
                    parser.word("unique")?;
                    unique = true;
                    unique_name = Some(constraint_name);
                } else if parser.peek_word("references") {
                    if foreign_keys.len() >= crate::tuple::MAX_COLUMNS {
                        return Err(DdlSqlError::InvalidStatement);
                    }
                    parser.word("references")?;
                    let referenced_schema = parser.identifier()?;
                    parser.take(Token::Dot)?;
                    let referenced_table = parser.identifier()?;
                    let referenced_columns = parse_column_name_list(&mut parser)?;
                    if referenced_columns.len() != 1 {
                        return Err(DdlSqlError::InvalidStatement);
                    }
                    foreign_keys.push(CreateTableForeignKeySql {
                        name: Some(constraint_name),
                        columns: vec![name.clone()],
                        referenced_schema,
                        referenced_table,
                        referenced_columns,
                    });
                } else {
                    return Err(DdlSqlError::InvalidStatement);
                }
            } else if parser.peek_word("primary") && !primary_key {
                parser.word("primary")?;
                parser.word("key")?;
                primary_key = true;
            } else if parser.peek_word("check") || parser.peek_word("constraint") {
                let constraint_name = if parser.peek_word("constraint") {
                    parser.word("constraint")?;
                    Some(parser.identifier()?)
                } else {
                    None
                };
                if check_constraints.len() >= crate::tuple::MAX_COLUMNS {
                    return Err(DdlSqlError::InvalidStatement);
                }
                let expression = parser.check_constraint_expression()?;
                validate_column_check_expression(&name, &expression)?;
                check_constraints.push(expression);
                check_constraint_names.push(constraint_name);
            } else if parser.peek_word("references") {
                if foreign_keys.len() >= crate::tuple::MAX_COLUMNS {
                    return Err(DdlSqlError::InvalidStatement);
                }
                parser.word("references")?;
                let referenced_schema = parser.identifier()?;
                parser.take(Token::Dot)?;
                let referenced_table = parser.identifier()?;
                let referenced_columns = parse_column_name_list(&mut parser)?;
                if referenced_columns.len() != 1 {
                    return Err(DdlSqlError::InvalidStatement);
                }
                foreign_keys.push(CreateTableForeignKeySql {
                    name: None,
                    columns: vec![name.clone()],
                    referenced_schema,
                    referenced_table,
                    referenced_columns,
                });
            } else {
                break;
            }
        }
        if generated_uuid_identity {
            if default.is_some() {
                return Err(DdlSqlError::InvalidStatement);
            }
            nullable = false;
        }
        if primary_key {
            nullable = false;
        }
        columns.push(AdminColumnDefinition {
            name: name.clone(),
            scalar_type,
            nullable,
            generated_uuid_identity,
            default,
        });
        if primary_key && primary_key_columns.replace(vec![name.clone()]).is_some() {
            return Err(DdlSqlError::InvalidStatement);
        }
        if primary_key {
            primary_key_name = primary_key_column_name;
        }
        if unique {
            let key_columns = vec![name.clone()];
            if unique_constraints.contains(&key_columns) {
                return Err(DdlSqlError::InvalidStatement);
            }
            unique_constraints.push(key_columns);
            unique_constraint_names.push(unique_name);
        }
        if parser.peek() == Some(&Token::Comma) {
            parser.position += 1;
            continue;
        }
        break;
    }
    parser.take(Token::RightParen)?;
    if parser.peek() == Some(&Token::Semicolon) {
        parser.position += 1;
    }
    if parser.peek().is_some() {
        return Err(DdlSqlError::InvalidStatement);
    }
    if let Some(key_columns) = &primary_key_columns {
        for column_name in key_columns {
            let column = columns
                .iter_mut()
                .find(|column| &column.name == column_name)
                .ok_or(DdlSqlError::InvalidStatement)?;
            column.nullable = false;
        }
    }
    if unique_constraints.iter().any(|key_columns| {
        key_columns
            .iter()
            .any(|name| !columns.iter().any(|column| &column.name == name))
    }) {
        return Err(DdlSqlError::InvalidStatement);
    }
    for expression in &check_constraints {
        validate_check_columns(expression, &columns)?;
    }
    if check_constraint_names.len() != check_constraints.len()
        || check_constraint_names
            .iter()
            .flatten()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != check_constraint_names.iter().flatten().count()
    {
        return Err(DdlSqlError::InvalidStatement);
    }
    let foreign_key_names = foreign_keys
        .iter()
        .map(|foreign_key| foreign_key.name.clone())
        .collect::<Vec<_>>();
    let named_names = primary_key_name
        .iter()
        .chain(unique_constraint_names.iter().flatten())
        .chain(check_constraint_names.iter().flatten())
        .chain(foreign_key_names.iter().flatten())
        .collect::<std::collections::HashSet<_>>();
    if named_names.len()
        != usize::from(primary_key_name.is_some())
            + unique_constraint_names.iter().flatten().count()
            + check_constraint_names.iter().flatten().count()
            + foreign_key_names.iter().flatten().count()
    {
        return Err(DdlSqlError::InvalidStatement);
    }
    if foreign_keys.iter().any(|foreign_key| {
        foreign_key
            .columns
            .iter()
            .any(|name| !columns.iter().any(|column| &column.name == name))
    }) {
        return Err(DdlSqlError::InvalidStatement);
    }
    Ok(CreateTableSql {
        schema,
        table,
        if_not_exists,
        columns,
        primary_key_columns,
        primary_key_name,
        unique_constraints,
        unique_constraint_names,
        check_constraints,
        check_constraint_names,
        foreign_key_names,
        foreign_keys,
    })
}

/// Parse a bounded ordinary or unique B-tree index definition.
pub fn parse_create_index(sql: &str) -> Result<CreateIndexSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("create")?;
    let unique = if parser.peek_word("unique") {
        parser.word("unique")?;
        true
    } else {
        false
    };
    parser.word("index")?;
    let if_not_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("not")?;
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
    if parser.peek_word("using") {
        parser.word("using")?;
        parser.word("btree")?;
    }
    parser.take(Token::LeftParen)?;
    let mut columns = vec![parser.identifier()?];
    while parser.peek() == Some(&Token::Comma) {
        parser.position += 1;
        if columns.len() >= crate::tuple::MAX_COLUMNS {
            return Err(DdlSqlError::InvalidStatement);
        }
        columns.push(parser.identifier()?);
    }
    parser.take(Token::RightParen)?;
    finish_statement(&mut parser)?;
    let mut seen = std::collections::HashSet::new();
    if columns.iter().any(|column| !seen.insert(column)) {
        return Err(DdlSqlError::InvalidStatement);
    }
    Ok(CreateIndexSql {
        schema,
        table,
        name,
        columns,
        unique,
        if_not_exists,
    })
}

/// Parse a bounded schema-qualified DROP INDEX with explicit RESTRICT.
pub fn parse_drop_index(sql: &str) -> Result<DropIndexSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("drop")?;
    parser.word("index")?;
    let if_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("exists")?;
        true
    } else {
        false
    };
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let name = parser.identifier()?;
    parser.word("restrict")?;
    finish_statement(&mut parser)?;
    Ok(DropIndexSql {
        schema,
        name,
        if_exists,
    })
}

/// Parse a bounded schema-qualified index rebuild request.
pub fn parse_rebuild_index(sql: &str) -> Result<RebuildIndexSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("rebuild")?;
    parser.word("index")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let name = parser.identifier()?;
    finish_statement(&mut parser)?;
    Ok(RebuildIndexSql { schema, name })
}

/// Parse an atomic index definition rebuild using a new ordered column set.
pub fn parse_alter_index_rebuild(sql: &str) -> Result<AlterIndexRebuildSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("index")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let name = parser.identifier()?;
    parser.word("rebuild")?;
    if parser.peek_word("using") {
        parser.word("using")?;
        parser.word("btree")?;
    }
    let unique = if parser.peek_word("unique") {
        parser.word("unique")?;
        true
    } else if parser.peek_word("nonunique") {
        parser.word("nonunique")?;
        false
    } else {
        return Err(DdlSqlError::InvalidStatement);
    };
    parser.take(Token::LeftParen)?;
    let mut columns = vec![parser.identifier()?];
    while parser.peek() == Some(&Token::Comma) {
        parser.position += 1;
        if columns.len() >= crate::tuple::MAX_COLUMNS {
            return Err(DdlSqlError::InvalidStatement);
        }
        columns.push(parser.identifier()?);
    }
    parser.take(Token::RightParen)?;
    finish_statement(&mut parser)?;
    let mut seen = std::collections::HashSet::new();
    if columns.iter().any(|column| !seen.insert(column)) {
        return Err(DdlSqlError::InvalidStatement);
    }
    Ok(AlterIndexRebuildSql {
        schema,
        name,
        columns,
        unique,
    })
}

/// Parse a bounded schema-qualified index rename.
pub fn parse_rename_index(sql: &str) -> Result<RenameIndexSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("index")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let name = parser.identifier()?;
    parser.word("rename")?;
    parser.word("to")?;
    let new_name = parser.identifier()?;
    finish_statement(&mut parser)?;
    Ok(RenameIndexSql {
        schema,
        name,
        new_name,
    })
}

/// Parse bounded schema creation with an optional UUID or typed owner.
pub fn parse_create_schema(sql: &str) -> Result<CreateSchemaSql, DdlSqlError> {
    if sql.is_empty() || sql.len() > MAX_DDL_BYTES || sql.as_bytes().contains(&0) {
        return Err(DdlSqlError::InvalidStatement);
    }
    let mut parser = Parser {
        tokens: tokenize(sql)?,
        position: 0,
    };
    parser.word("create")?;
    parser.word("schema")?;
    let if_not_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("not")?;
        parser.word("exists")?;
        true
    } else {
        false
    };
    let schema = parser.identifier()?;
    let owner = if parser.peek_word("authorization") {
        parser.word("authorization")?;
        if let Some(Token::String(value)) = parser.peek() {
            let owner_id = parse_uuid(value)?;
            if owner_id == [0; 16] {
                return Err(DdlSqlError::InvalidStatement);
            }
            parser.position += 1;
            CreateSchemaOwnerSql::Id(owner_id)
        } else {
            let kind = if parser.peek_word("user") {
                parser.word("user")?;
                SqlIdentityKind::User
            } else if parser.peek_word("role") {
                parser.word("role")?;
                SqlIdentityKind::Role
            } else {
                SqlIdentityKind::Either
            };
            CreateSchemaOwnerSql::Identity {
                name: parser.identifier()?,
                kind,
            }
        }
    } else {
        CreateSchemaOwnerSql::CurrentUser
    };
    finish_statement(&mut parser)?;
    Ok(CreateSchemaSql {
        schema,
        owner,
        if_not_exists,
    })
}

pub fn parse_rename_schema(sql: &str) -> Result<RenameSchemaSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("schema")?;
    let schema = parser.identifier()?;
    parser.word("rename")?;
    parser.word("to")?;
    let new_name = parser.identifier()?;
    finish_statement(&mut parser)?;
    Ok(RenameSchemaSql { schema, new_name })
}

pub fn parse_schema_owner_transfer(sql: &str) -> Result<OwnerTransferSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("schema")?;
    let schema = parser.identifier()?;
    parser.word("owner")?;
    parser.word("to")?;
    let owner_kind = if parser.peek_word("user") {
        parser.word("user")?;
        SqlIdentityKind::User
    } else if parser.peek_word("role") {
        parser.word("role")?;
        SqlIdentityKind::Role
    } else {
        SqlIdentityKind::Either
    };
    let owner_name = parser.identifier()?;
    finish_statement(&mut parser)?;
    Ok(OwnerTransferSql {
        schema,
        table: None,
        owner_name,
        owner_kind,
    })
}

pub fn parse_drop_schema(sql: &str) -> Result<DropSchemaSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("drop")?;
    parser.word("schema")?;
    let if_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("exists")?;
        true
    } else {
        false
    };
    let schema = parser.identifier()?;
    parser.word("restrict")?;
    finish_statement(&mut parser)?;
    Ok(DropSchemaSql { schema, if_exists })
}

pub fn parse_rename_table(sql: &str) -> Result<RenameTableSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("rename")?;
    parser.word("to")?;
    let new_name = parser.identifier()?;
    finish_statement(&mut parser)?;
    Ok(RenameTableSql {
        schema,
        table,
        new_name,
    })
}

pub fn parse_table_owner_transfer(sql: &str) -> Result<OwnerTransferSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("owner")?;
    parser.word("to")?;
    let owner_kind = if parser.peek_word("user") {
        parser.word("user")?;
        SqlIdentityKind::User
    } else {
        parser.word("role")?;
        SqlIdentityKind::Role
    };
    let owner_name = parser.identifier()?;
    finish_statement(&mut parser)?;
    Ok(OwnerTransferSql {
        schema,
        table: Some(table),
        owner_name,
        owner_kind,
    })
}

pub fn parse_drop_table(sql: &str) -> Result<DropTableSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("drop")?;
    parser.word("table")?;
    let if_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("exists")?;
        true
    } else {
        false
    };
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("restrict")?;
    finish_statement(&mut parser)?;
    Ok(DropTableSql {
        schema,
        table,
        if_exists,
    })
}

/// Parse `TRUNCATE TABLE schema.table RESTRICT`.
pub fn parse_truncate_table(sql: &str) -> Result<TruncateTableSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("truncate")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("restrict")?;
    finish_statement(&mut parser)?;
    Ok(TruncateTableSql { schema, table })
}

/// Parse `ALTER TABLE schema.table ADD {PRIMARY KEY|UNIQUE} (columns...)`.
pub fn parse_add_key_constraint(sql: &str) -> Result<AddKeyConstraintSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("add")?;
    let name = if parser.peek_word("constraint") {
        parser.word("constraint")?;
        Some(parser.identifier()?)
    } else {
        None
    };
    let kind = if parser.peek_word("primary") {
        parser.word("primary")?;
        parser.word("key")?;
        KeyConstraintKind::PrimaryKey
    } else {
        parser.word("unique")?;
        KeyConstraintKind::Unique
    };
    parser.take(Token::LeftParen)?;
    let mut columns = vec![parser.identifier()?];
    while parser.peek() == Some(&Token::Comma) {
        parser.position += 1;
        columns.push(parser.identifier()?);
    }
    parser.take(Token::RightParen)?;
    finish_statement(&mut parser)?;
    if columns.len() > crate::tuple::MAX_COLUMNS
        || columns
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != columns.len()
    {
        return Err(DdlSqlError::InvalidStatement);
    }
    Ok(AddKeyConstraintSql {
        schema,
        table,
        name,
        columns,
        kind,
    })
}

/// Parse qualified `ALTER TABLE ... ADD FOREIGN KEY (...) REFERENCES ...`.
pub fn parse_add_foreign_key(sql: &str) -> Result<AddForeignKeySql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("add")?;
    let name = if parser.peek_word("constraint") {
        parser.word("constraint")?;
        Some(parser.identifier()?)
    } else {
        None
    };
    parser.word("foreign")?;
    parser.word("key")?;
    let columns = parse_column_name_list(&mut parser)?;
    parser.word("references")?;
    let referenced_schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let referenced_table = parser.identifier()?;
    let referenced_columns = parse_column_name_list(&mut parser)?;
    finish_statement(&mut parser)?;
    if columns.len() > crate::tuple::MAX_COLUMNS
        || columns.len() != referenced_columns.len()
        || has_duplicates(&columns)
        || has_duplicates(&referenced_columns)
    {
        return Err(DdlSqlError::InvalidStatement);
    }
    Ok(AddForeignKeySql {
        schema,
        table,
        name,
        columns,
        referenced_schema,
        referenced_table,
        referenced_columns,
    })
}

/// Parse qualified `ALTER TABLE ... DROP CONSTRAINT name|'generated-id'`.
pub fn parse_drop_constraint(sql: &str) -> Result<DropConstraintSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("drop")?;
    parser.word("constraint")?;
    let if_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("exists")?;
        true
    } else {
        false
    };
    let (constraint_id, constraint_name) = match parser.peek() {
        Some(Token::String(value)) => {
            let parsed = parse_uuid(value)?;
            parser.position += 1;
            (parsed, None)
        }
        Some(Token::Word(_)) => ([0; 16], Some(parser.identifier()?)),
        _ => return Err(DdlSqlError::InvalidStatement),
    };
    finish_statement(&mut parser)?;
    if constraint_id == [0; 16] && constraint_name.is_none() {
        return Err(DdlSqlError::InvalidStatement);
    }
    Ok(DropConstraintSql {
        schema,
        table,
        constraint_id,
        constraint_name,
        if_exists,
    })
}

/// Parse `ALTER TABLE schema.table ADD [CONSTRAINT name] CHECK (expression)` using a bounded
/// Boolean grammar over columns, typed comparisons, literals, and NULL tests.
pub fn parse_add_check_constraint(sql: &str) -> Result<AddCheckConstraintSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("add")?;
    let name = if parser.peek_word("constraint") {
        parser.word("constraint")?;
        Some(parser.identifier()?)
    } else {
        None
    };
    let expression = parser.check_constraint_expression()?;
    finish_statement(&mut parser)?;
    Ok(AddCheckConstraintSql {
        schema,
        table,
        name,
        expression,
    })
}

/// Parse `ALTER TABLE schema.table ALTER COLUMN name {SET|DROP} NOT NULL`.
pub fn parse_alter_column_nullability(sql: &str) -> Result<AlterColumnNullabilitySql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("alter")?;
    parser.word("column")?;
    let column = parser.identifier()?;
    let nullable = if parser.peek_word("set") {
        parser.word("set")?;
        parser.word("not")?;
        parser.word("null")?;
        false
    } else {
        parser.word("drop")?;
        parser.word("not")?;
        parser.word("null")?;
        true
    };
    finish_statement(&mut parser)?;
    Ok(AlterColumnNullabilitySql {
        schema,
        table,
        column,
        nullable,
    })
}

/// Parse the bounded, lossless `VARCHAR` widening form of `ALTER COLUMN TYPE`.
pub fn parse_widen_varchar(sql: &str) -> Result<WidenVarcharSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("alter")?;
    parser.word("column")?;
    let column = parser.identifier()?;
    parser.word("type")?;
    parser.word("varchar")?;
    parser.take(Token::LeftParen)?;
    let max_chars = parser.number()?;
    parser.take(Token::RightParen)?;
    if max_chars == 0 {
        return Err(DdlSqlError::InvalidStatement);
    }
    finish_statement(&mut parser)?;
    Ok(WidenVarcharSql {
        schema,
        table,
        column,
        max_chars,
    })
}

/// Parse `ALTER TABLE ... ALTER COLUMN ... SET|DROP DEFAULT` with a
/// context-independent literal. The authenticated client binds it to the
/// declared column type after metadata resolution.
pub fn parse_alter_column_default(sql: &str) -> Result<AlterColumnDefaultSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("alter")?;
    parser.word("column")?;
    let column = parser.identifier()?;
    let default = if parser.peek_word("set") {
        parser.word("set")?;
        parser.word("default")?;
        Some(parser.default_literal()?)
    } else {
        parser.word("drop")?;
        parser.word("default")?;
        None
    };
    finish_statement(&mut parser)?;
    Ok(AlterColumnDefaultSql {
        schema,
        table,
        column,
        default,
    })
}

/// Parse one typed `ALTER TABLE schema.table ADD COLUMN` definition.
pub fn parse_add_column(sql: &str) -> Result<AddColumnSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("add")?;
    parser.word("column")?;
    let if_not_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("not")?;
        parser.word("exists")?;
        true
    } else {
        false
    };
    let name = parser.identifier()?;
    let scalar_type = parser.scalar_type()?;
    let mut nullable = true;
    let mut default = None;
    loop {
        if parser.peek_word("not") && nullable {
            parser.word("not")?;
            parser.word("null")?;
            nullable = false;
        } else if parser.peek_word("default") && default.is_none() {
            parser.word("default")?;
            default = Some(parser.default_value(scalar_type)?);
        } else {
            break;
        }
    }
    if matches!(default, Some(AdminColumnDefault::CurrentPrincipalId))
        || matches!(default, Some(AdminColumnDefault::Null)) && !nullable
        || !nullable && default.is_none()
    {
        return Err(DdlSqlError::InvalidStatement);
    }
    finish_statement(&mut parser)?;
    Ok(AddColumnSql {
        schema,
        table,
        if_not_exists,
        column: AdminColumnDefinition {
            name,
            scalar_type,
            nullable,
            generated_uuid_identity: false,
            default,
        },
    })
}

/// Parse `ALTER TABLE schema.table RENAME COLUMN name TO new_name`.
pub fn parse_rename_column(sql: &str) -> Result<RenameColumnSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("rename")?;
    parser.word("column")?;
    let column = parser.identifier()?;
    parser.word("to")?;
    let new_name = parser.identifier()?;
    finish_statement(&mut parser)?;
    Ok(RenameColumnSql {
        schema,
        table,
        column,
        new_name,
    })
}

/// Parse `ALTER TABLE schema.table DROP COLUMN name RESTRICT`.
pub fn parse_drop_column(sql: &str) -> Result<DropColumnSql, DdlSqlError> {
    let mut parser = statement_parser(sql)?;
    parser.word("alter")?;
    parser.word("table")?;
    let schema = parser.identifier()?;
    parser.take(Token::Dot)?;
    let table = parser.identifier()?;
    parser.word("drop")?;
    let if_exists = if parser.peek_word("if") {
        parser.word("if")?;
        parser.word("exists")?;
        true
    } else {
        false
    };
    parser.word("column")?;
    let column = parser.identifier()?;
    parser.word("restrict")?;
    finish_statement(&mut parser)?;
    Ok(DropColumnSql {
        schema,
        table,
        column,
        if_exists,
    })
}
