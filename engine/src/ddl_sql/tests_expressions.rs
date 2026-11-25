use super::*;
use crate::client_submit::AdminColumnDefault;
use crate::tuple::{ScalarType, ScalarValue};

#[test]
fn binds_policy_context_functions_to_typed_authority_nodes() {
    use crate::authorization::Expr;
    let current = parse_add_check_constraint(
        "ALTER TABLE app.records ADD CHECK (owner = CURRENT_PRINCIPAL_ID())",
    )
    .unwrap();
    let expression = bind_policy_expression(
        &current.expression,
        |name| (name == "owner").then_some((0, ScalarType::Uuid)),
        |_| None,
    )
    .unwrap();
    assert_eq!(
        expression,
        Expr::Equal(Box::new(Expr::Field(0)), Box::new(Expr::CurrentPrincipalId),)
    );
    let role =
        parse_add_check_constraint("ALTER TABLE app.records ADD CHECK (HAS_ROLE('payroll'))")
            .unwrap();
    assert_eq!(
        bind_policy_expression(
            &role.expression,
            |_| None,
            |name| { (name == "payroll").then_some([7; 16]) }
        )
        .unwrap(),
        Expr::HasRole([7; 16])
    );
    assert!(bind_check_expression(&current.expression, |_| None).is_err());
    assert!(bind_policy_expression(&role.expression, |_| None, |_| None).is_err());
}

#[test]
fn parses_bounded_check_alterations_and_rejects_other_expression_syntax() {
    assert_eq!(
            parse_add_check_constraint("ALTER TABLE app.events ADD CHECK (active AND (deleted IS NULL OR deleted IS NOT NULL));").unwrap(),
            AddCheckConstraintSql {
                schema: "app".into(),
                table: "events".into(),
                name: None,
                expression: CheckExpressionSql::And(vec![
                    CheckExpressionSql::Column("active".into()),
                    CheckExpressionSql::Or(vec![
                        CheckExpressionSql::IsNull(Box::new(CheckExpressionSql::Column(
                            "deleted".into(),
                        ))),
                        CheckExpressionSql::IsNotNull(Box::new(CheckExpressionSql::Column(
                            "deleted".into(),
                        ))),
                    ]),
                ]),
            }
        );
    let named = parse_add_check_constraint(
        "ALTER TABLE app.events ADD CONSTRAINT positive_amount CHECK (amount > 0)",
    )
    .unwrap();
    assert_eq!(named.name.as_deref(), Some("positive_amount"));
    let named_drop =
        parse_drop_constraint("ALTER TABLE app.orders DROP CONSTRAINT positive_amount").unwrap();
    assert_eq!(
        named_drop.constraint_name.as_deref(),
        Some("positive_amount")
    );
    assert_eq!(named_drop.constraint_id, [0; 16]);
    for sql in [
        "ALTER TABLE app.events ADD CHECK (active); ALTER TABLE app.events ADD CHECK (active)",
        "ALTER TABLE events ADD CHECK (active)",
        "ALTER TABLE app.events ADD CONSTRAINT CHECK (active)",
        "ALTER TABLE app.events ADD CHECK (missing IS NULL OR)",
    ] {
        assert!(parse_add_check_constraint(sql).is_err(), "accepted {sql}");
    }
    assert!(parse_add_check_constraint("ALTER TABLE app.events ADD CHECK (amount >= 0)").is_ok());
}

#[test]
fn parses_column_nullability_alterations_only() {
    assert_eq!(
        parse_alter_column_nullability(
            "ALTER TABLE app.employees ALTER COLUMN active SET NOT NULL;"
        )
        .unwrap(),
        AlterColumnNullabilitySql {
            schema: "app".into(),
            table: "employees".into(),
            column: "active".into(),
            nullable: false
        }
    );
    assert!(
        parse_alter_column_nullability(
            "ALTER TABLE app.employees ALTER COLUMN active DROP NOT NULL"
        )
        .unwrap()
        .nullable
    );
    for sql in [
        "ALTER TABLE employees ALTER COLUMN active SET NOT NULL",
        "ALTER TABLE app.employees ALTER active SET NOT NULL",
        "ALTER TABLE app.employees ALTER COLUMN active SET NULL",
        "ALTER TABLE app.employees ALTER COLUMN active SET NOT NULL; ALTER TABLE app.employees ALTER COLUMN active DROP NOT NULL",
    ] {
        assert!(
            parse_alter_column_nullability(sql).is_err(),
            "accepted {sql}"
        );
    }
}

#[test]
fn parses_bounded_varchar_widening_syntax() {
    assert_eq!(
        parse_widen_varchar("ALTER TABLE app.events ALTER COLUMN label TYPE VARCHAR(120);")
            .unwrap(),
        WidenVarcharSql {
            schema: "app".into(),
            table: "events".into(),
            column: "label".into(),
            max_chars: 120,
        }
    );
    for sql in [
        "ALTER TABLE app.events ALTER COLUMN label TYPE VARCHAR(0)",
        "ALTER TABLE events ALTER COLUMN label TYPE VARCHAR(120)",
        "ALTER TABLE app.events ALTER COLUMN label TYPE INTEGER",
        "ALTER TABLE app.events ALTER COLUMN label TYPE VARCHAR(120) USING label",
        "ALTER TABLE app.events ALTER COLUMN label TYPE VARCHAR(120); ALTER TABLE app.events ALTER COLUMN label TYPE VARCHAR(140)",
    ] {
        assert!(parse_widen_varchar(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_typed_column_default_alterations() {
    assert_eq!(
        parse_alter_column_default("ALTER TABLE app.events ALTER COLUMN amount SET DEFAULT -12.30")
            .unwrap(),
        AlterColumnDefaultSql {
            schema: "app".into(),
            table: "events".into(),
            column: "amount".into(),
            default: Some(DefaultLiteralSql::Number("-12.30".into())),
        }
    );
    assert_eq!(
        parse_alter_column_default("ALTER TABLE app.events ALTER COLUMN amount DROP DEFAULT;")
            .unwrap()
            .default,
        None
    );
    for (sql, expected) in [
        (
            "ALTER TABLE app.events ALTER COLUMN label SET DEFAULT 'ready'",
            DefaultLiteralSql::String("ready".into()),
        ),
        (
            "ALTER TABLE app.events ALTER COLUMN enabled SET DEFAULT TRUE",
            DefaultLiteralSql::Boolean(true),
        ),
        (
            "ALTER TABLE app.events ALTER COLUMN payload SET DEFAULT X'00ff'",
            DefaultLiteralSql::Bytea(vec![0, 255]),
        ),
        (
            "ALTER TABLE app.events ALTER COLUMN owner SET DEFAULT CURRENT_PRINCIPAL_ID()",
            DefaultLiteralSql::CurrentPrincipalId,
        ),
        (
            "ALTER TABLE app.events ALTER COLUMN label SET DEFAULT NULL",
            DefaultLiteralSql::Null,
        ),
    ] {
        assert_eq!(
            parse_alter_column_default(sql).unwrap().default,
            Some(expected)
        );
    }
    let decimal = DefaultLiteralSql::Number("-12.30".into());
    assert_eq!(
        bind_default_literal(
            &decimal,
            ScalarType::Decimal {
                precision: 5,
                scale: 2,
            },
        )
        .unwrap(),
        AdminColumnDefault::Literal(ScalarValue::Decimal(-1230))
    );
    assert!(bind_default_literal(&decimal, ScalarType::Integer).is_err());
    assert!(
        bind_default_literal(&DefaultLiteralSql::CurrentPrincipalId, ScalarType::Integer).is_err()
    );
    for sql in [
        "ALTER TABLE app.events ALTER COLUMN label SET DEFAULT",
        "ALTER TABLE app.events ALTER COLUMN label DROP NOT NULL",
        "ALTER TABLE events ALTER COLUMN label DROP DEFAULT",
        "ALTER TABLE app.events ALTER COLUMN label DROP DEFAULT; ALTER TABLE app.events ALTER COLUMN label DROP DEFAULT",
    ] {
        assert!(parse_alter_column_default(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_create_index_sql() {
    assert_eq!(
        parse_create_index(
            "CREATE INDEX employee_department ON app.employees USING BTREE (department_id, name);"
        )
        .unwrap(),
        CreateIndexSql {
            schema: "app".into(),
            table: "employees".into(),
            name: "employee_department".into(),
            columns: vec!["department_id".into(), "name".into()],
            unique: false,
            if_not_exists: false,
        }
    );
    let unique = parse_create_index(
        "CREATE UNIQUE INDEX employee_department ON app.employees (department_id, name)",
    )
    .unwrap();
    assert!(unique.unique);
    assert_eq!(unique.columns, ["department_id", "name"]);
    let conditional = parse_create_index(
            "CREATE UNIQUE INDEX IF NOT EXISTS employee_department ON app.employees (department_id, name)",
        )
        .unwrap();
    assert!(conditional.unique);
    assert!(conditional.if_not_exists);
    for sql in [
        "CREATE INDEX employee_department ON employees (department_id)",
        "CREATE INDEX employee_department ON app.employees USING HASH (department_id)",
        "CREATE INDEX employee_department ON app.employees (department_id, department_id)",
        "CREATE INDEX employee_department ON app.employees ()",
        "CREATE INDEX employee_department ON app.employees (department_id); DROP INDEX app.employee_department RESTRICT",
    ] {
        assert!(parse_create_index(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_drop_index_sql() {
    assert_eq!(
        parse_drop_index("DROP INDEX app.employee_department RESTRICT;").unwrap(),
        DropIndexSql {
            schema: "app".into(),
            name: "employee_department".into(),
            if_exists: false,
        }
    );
    for sql in [
        "DROP INDEX employee_department RESTRICT",
        "DROP INDEX app.employee_department",
        "DROP INDEX app.employee_department CASCADE",
        "DROP INDEX app.employee_department RESTRICT; DROP TABLE app.users RESTRICT",
    ] {
        assert!(parse_drop_index(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_rebuild_index_sql() {
    assert_eq!(
        parse_rebuild_index("REBUILD INDEX app.employee_department;").unwrap(),
        RebuildIndexSql {
            schema: "app".into(),
            name: "employee_department".into(),
        }
    );
    for sql in [
        "REBUILD INDEX employee_department",
        "REBUILD INDEX app.employee_department RESTRICT",
        "REBUILD INDEX app.employee_department; DROP TABLE app.users RESTRICT",
    ] {
        assert!(parse_rebuild_index(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_alter_index_rebuild_sql() {
    assert_eq!(
            parse_alter_index_rebuild(
                "ALTER INDEX app.employee_department REBUILD USING BTREE UNIQUE (department_id, employee_id);"
            )
            .unwrap(),
            AlterIndexRebuildSql {
                schema: "app".into(),
                name: "employee_department".into(),
                columns: vec!["department_id".into(), "employee_id".into()],
                unique: true,
            }
        );
    for sql in [
        "ALTER INDEX employee_department REBUILD UNIQUE (department_id)",
        "ALTER INDEX app.employee_department REBUILD (department_id)",
        "ALTER INDEX app.employee_department REBUILD USING HASH UNIQUE (department_id)",
        "ALTER INDEX app.employee_department REBUILD UNIQUE (department_id, department_id)",
        "ALTER INDEX app.employee_department REBUILD NONUNIQUE ()",
        "ALTER INDEX app.employee_department REBUILD UNIQUE (department_id); DROP INDEX app.other RESTRICT",
    ] {
        assert!(parse_alter_index_rebuild(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_rename_index_sql() {
    assert_eq!(
        parse_rename_index("ALTER INDEX app.employee_department RENAME TO employee_team;").unwrap(),
        RenameIndexSql {
            schema: "app".into(),
            name: "employee_department".into(),
            new_name: "employee_team".into(),
        }
    );
    for sql in [
        "ALTER INDEX employee_department RENAME TO employee_team",
        "ALTER INDEX app.employee_department RENAME employee_team",
        "ALTER INDEX app.employee_department RENAME TO employee_team; DROP INDEX app.other RESTRICT",
    ] {
        assert!(parse_rename_index(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_identity_creation_sql() {
    assert_eq!(
        parse_create_identity("CREATE USER analyst LOGIN;").unwrap(),
        CreateIdentitySql {
            kind: CreateIdentityKind::User,
            if_not_exists: false,
            name: "analyst".into(),
            login_enabled: true,
        }
    );
    assert_eq!(
        parse_create_identity("CREATE ROLE reporting").unwrap(),
        CreateIdentitySql {
            kind: CreateIdentityKind::Role,
            if_not_exists: false,
            name: "reporting".into(),
            login_enabled: false,
        }
    );
    assert!(
        parse_create_identity("CREATE USER IF NOT EXISTS analyst NOLOGIN")
            .unwrap()
            .if_not_exists
    );
    assert!(
        parse_create_identity("CREATE ROLE IF NOT EXISTS reporting")
            .unwrap()
            .if_not_exists
    );
    for sql in [
        "CREATE USER",
        "CREATE USER IF EXISTS analyst LOGIN",
        "CREATE GROUP reporting",
        "CREATE USER analyst",
        "CREATE USER app.analyst",
        "CREATE ROLE reporting LOGIN",
        "CREATE ROLE reporting; CREATE USER analyst",
    ] {
        assert!(parse_create_identity(sql).is_err(), "accepted {sql}");
    }
    assert!(
        !parse_create_identity("CREATE USER service_loader NOLOGIN")
            .unwrap()
            .login_enabled
    );
}

#[test]
fn parses_bounded_identity_rename_sql() {
    assert_eq!(
        parse_rename_identity("ALTER USER analyst RENAME TO analyst_admin;").unwrap(),
        RenameIdentitySql {
            kind: CreateIdentityKind::User,
            old_name: "analyst".into(),
            new_name: "analyst_admin".into(),
        }
    );
    assert_eq!(
        parse_rename_identity("ALTER ROLE reporting RENAME TO report_readers").unwrap(),
        RenameIdentitySql {
            kind: CreateIdentityKind::Role,
            old_name: "reporting".into(),
            new_name: "report_readers".into(),
        }
    );
    for sql in [
        "ALTER USER RENAME TO analyst_admin",
        "ALTER USER analyst RENAME analyst_admin",
        "ALTER USER analyst RENAME TO app.analyst_admin",
        "ALTER USER analyst RENAME TO analyst_admin; DROP USER other RESTRICT",
    ] {
        assert!(parse_rename_identity(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_identity_drop_sql() {
    assert_eq!(
        parse_drop_identity("DROP USER service_loader RESTRICT;").unwrap(),
        DropIdentitySql {
            kind: CreateIdentityKind::User,
            name: "service_loader".into(),
            if_exists: false,
        }
    );
    assert_eq!(
        parse_drop_identity("DROP ROLE old_role RESTRICT").unwrap(),
        DropIdentitySql {
            kind: CreateIdentityKind::Role,
            name: "old_role".into(),
            if_exists: false,
        }
    );
    for sql in [
        "DROP USER service_loader",
        "DROP USER service_loader CASCADE",
        "DROP ROLE RESTRICT",
        "DROP ROLE old_role RESTRICT; CREATE ROLE other",
    ] {
        assert!(parse_drop_identity(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_user_login_sql() {
    assert_eq!(
        parse_set_user_login("ALTER USER analyst NOLOGIN;").unwrap(),
        SetUserLoginSql {
            name: "analyst".into(),
            enabled: false,
        }
    );
    assert_eq!(
        parse_set_user_login("ALTER USER analyst LOGIN").unwrap(),
        SetUserLoginSql {
            name: "analyst".into(),
            enabled: true,
        }
    );
    for sql in [
        "ALTER ROLE analyst LOGIN",
        "ALTER USER analyst",
        "ALTER USER analyst LOGIN; ALTER USER analyst NOLOGIN",
    ] {
        assert!(parse_set_user_login(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_role_membership_sql() {
    assert_eq!(
        parse_role_membership("GRANT managers TO USER alice;").unwrap(),
        RoleMembershipSql {
            grant: true,
            role_name: "managers".into(),
            member_name: "alice".into(),
            member_is_role: false,
        }
    );
    assert_eq!(
        parse_role_membership("REVOKE managers FROM ROLE app_owner").unwrap(),
        RoleMembershipSql {
            grant: false,
            role_name: "managers".into(),
            member_name: "app_owner".into(),
            member_is_role: true,
        }
    );
    for sql in [
        "GRANT managers USER alice",
        "REVOKE managers TO USER alice",
        "GRANT managers TO alice",
        "GRANT managers TO USER alice WITH ADMIN OPTION",
        "GRANT managers TO USER alice; REVOKE managers FROM USER alice",
    ] {
        assert!(parse_role_membership(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_system_privilege_sql() {
    assert_eq!(
        parse_system_privilege("GRANT SYSTEM PRIVILEGE MANAGE_ROLES TO ROLE administrators;")
            .unwrap(),
        SystemPrivilegeSql {
            grant: true,
            privilege: "manage_roles".into(),
            grantee_name: "administrators".into(),
            grantee_is_role: true,
        }
    );
    assert_eq!(
        parse_system_privilege("REVOKE SYSTEM PRIVILEGE CREATE_SCHEMA FROM USER deployer").unwrap(),
        SystemPrivilegeSql {
            grant: false,
            privilege: "create_schema".into(),
            grantee_name: "deployer".into(),
            grantee_is_role: false,
        }
    );
    assert_eq!(
        parse_system_privilege("GRANT MANAGE_USERS ON DATABASE TO USER account_admin;").unwrap(),
        SystemPrivilegeSql {
            grant: true,
            privilege: "manage_users".into(),
            grantee_name: "account_admin".into(),
            grantee_is_role: false,
        }
    );
    assert_eq!(
        parse_system_privilege("REVOKE CREATE_SCHEMA ON DATABASE FROM ROLE deployers").unwrap(),
        SystemPrivilegeSql {
            grant: false,
            privilege: "create_schema".into(),
            grantee_name: "deployers".into(),
            grantee_is_role: true,
        }
    );
    for sql in [
        "GRANT SYSTEM PRIVILEGE SELECT TO USER alice",
        "GRANT SYSTEM PRIVILEGE MANAGE_USERS USER alice",
        "REVOKE SYSTEM PRIVILEGE CREATE_SCHEMA TO USER alice",
        "GRANT SYSTEM PRIVILEGE MANAGE_ROLES TO ROLE admins WITH ADMIN OPTION",
        "GRANT SYSTEM PRIVILEGE MANAGE_USERS TO USER alice; REVOKE SYSTEM PRIVILEGE MANAGE_USERS FROM USER alice",
        "GRANT MANAGE_USERS TO USER alice",
        "GRANT SELECT ON DATABASE TO USER alice",
        "GRANT MANAGE_USERS ON SCHEMA app TO USER alice",
        "REVOKE MANAGE_ROLES ON DATABASE TO ROLE admins",
    ] {
        assert!(parse_system_privilege(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_object_privilege_sql() {
    assert_eq!(
        parse_object_privilege("GRANT USAGE ON SCHEMA app TO ROLE managers;").unwrap(),
        ObjectPrivilegeSql {
            grant: true,
            privileges: vec!["usage".into()],
            schema_name: "app".into(),
            table_name: None,
            column_names: vec![],
            grantee_name: "managers".into(),
            grantee_is_role: true,
        }
    );
    assert_eq!(
        parse_object_privilege("REVOKE SELECT ON TABLE app.employees FROM USER reader").unwrap(),
        ObjectPrivilegeSql {
            grant: false,
            privileges: vec!["select".into()],
            schema_name: "app".into(),
            table_name: Some("employees".into()),
            column_names: vec![],
            grantee_name: "reader".into(),
            grantee_is_role: false,
        }
    );
    assert_eq!(
            parse_object_privilege(
                "GRANT ALTER, INDEX, MANAGE_POLICIES, MANAGE_GRANTS ON TABLE app.employees TO ROLE app_owner"
            )
            .unwrap()
            .privileges,
            ["alter", "index", "manage_policies", "manage_grants"]
        );
    let mixed_scope = parse_object_privilege(
        "GRANT ALTER, REFERENCES (employee_id) ON TABLE app.employees TO ROLE app_owner",
    )
    .unwrap();
    assert_eq!(mixed_scope.privileges, ["alter", "references"]);
    assert_eq!(mixed_scope.column_names, ["employee_id"]);
    assert_eq!(
        parse_object_privilege("GRANT USAGE, CREATE ON SCHEMA app TO ROLE app_owner")
            .unwrap()
            .privileges,
        ["usage", "create"]
    );
    for sql in [
        "GRANT SELECT ON employees TO USER reader",
        "GRANT USAGE ON TABLE app.employees TO USER reader",
        "GRANT SELECT ON SCHEMA app TO USER reader WITH GRANT OPTION",
        "REVOKE USAGE ON SCHEMA app TO ROLE managers",
        "GRANT DELETE ON TABLE app.t TO ROLE r; REVOKE DELETE ON TABLE app.t FROM ROLE r",
        "GRANT USAGE, SELECT ON TABLE app.t TO ROLE r",
        "GRANT SELECT, SELECT ON TABLE app.t TO ROLE r",
        "GRANT SELECT, UNKNOWN ON TABLE app.t TO ROLE r",
        "GRANT SELECT, UPDATE (a) ON TABLE app.t TO ROLE r",
    ] {
        assert!(parse_object_privilege(sql).is_err(), "accepted {sql}");
    }
    assert_eq!(
        parse_object_privilege("GRANT SELECT (salary) ON TABLE app.employees TO ROLE readers")
            .unwrap()
            .column_names,
        vec!["salary"]
    );
    for sql in [
        "GRANT DELETE (salary) ON TABLE app.t TO ROLE r",
        "GRANT SELECT (a) ON SCHEMA app TO ROLE r",
        "GRANT SELECT () ON TABLE app.t TO ROLE r",
        "GRANT SELECT (a, a) ON TABLE app.t TO ROLE r",
    ] {
        assert!(parse_object_privilege(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_multiple_column_object_privilege_targets() {
    assert_eq!(
        parse_object_privilege(
            "GRANT SELECT (first_name, last_name) ON TABLE app.people TO ROLE readers"
        )
        .unwrap()
        .column_names,
        vec!["first_name", "last_name"]
    );
    let too_many_columns = (0..65)
        .map(|index| format!("column_{index}"))
        .collect::<Vec<_>>()
        .join(", ");
    assert!(
        parse_object_privilege(&format!(
            "GRANT SELECT ({too_many_columns}) ON TABLE app.people TO ROLE readers"
        ))
        .is_err()
    );
}

#[test]
fn parses_bounded_row_and_cell_policy_sql() {
    let row = parse_create_policy(
            "CREATE ROW POLICY visible ON app.records FOR SELECT TO ROLE readers, USER auditor AS ALLOW USING (enabled = TRUE AND owner = CURRENT_PRINCIPAL_ID())",
        )
        .unwrap();
    assert_eq!(row.name, "visible");
    assert!(!row.replace_existing);
    assert!(!row.is_cell);
    assert_eq!(row.operation, "select");
    assert_eq!(row.recipients.len(), 2);
    assert!(row.with_check_expression.is_none());
    let cell = parse_create_policy(
            "CREATE CELL POLICY salary_read ON app.records (salary) FOR SELECT TO ROLE managers AS RESTRICT USING (HAS_ROLE('payroll') OR salary IS NULL);",
        )
        .unwrap();
    assert!(cell.is_cell);
    assert_eq!(cell.columns, ["salary"]);
    let replacement = parse_create_policy(
            "CREATE OR REPLACE ROW POLICY visible ON app.records FOR SELECT TO ROLE readers AS ALLOW USING (enabled = TRUE)",
        )
        .unwrap();
    assert!(replacement.replace_existing);
    assert_eq!(replacement.name, "visible");
    for sql in [
        "CREATE ROW POLICY p ON app.t FOR SELECT TO ROLE r AS ALLOW",
        "CREATE ROW POLICY p ON app.t FOR UPDATE TO ROLE r AS ALLOW USING (TRUE)",
        "CREATE ROW POLICY p ON app.t FOR SELECT TO ROLE r AS ALLOW WITH CHECK (TRUE)",
        "CREATE CELL POLICY p ON app.t (x) FOR DELETE TO ROLE r AS ALLOW USING (TRUE)",
        "CREATE CELL POLICY p ON app.t (x, x) FOR SELECT TO ROLE r AS ALLOW USING (TRUE)",
        "CREATE ROW POLICY p ON app.t FOR INSERT TO ROLE r, ROLE r AS ALLOW WITH CHECK (TRUE)",
        "CREATE ROW POLICY p ON app.t FOR SELECT TO ROLE r AS ALLOW USING (random() > 0)",
        "CREATE ROW POLICY p ON app.t FOR SELECT TO ROLE r AS ALLOW USING (TRUE); CREATE ROLE other",
    ] {
        assert!(parse_create_policy(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_bounded_row_and_cell_policy_drop_sql() {
    let row = parse_drop_policy("DROP ROW POLICY visible ON app.records RESTRICT;").unwrap();
    assert_eq!(row.schema, "app");
    assert_eq!(row.table, "records");
    assert_eq!(row.name, "visible");
    assert!(!row.is_cell);
    let cell = parse_drop_policy("DROP CELL POLICY salary_read ON app.records RESTRICT").unwrap();
    assert!(cell.is_cell);
    for sql in [
        "DROP POLICY visible ON app.records RESTRICT",
        "DROP ROW POLICY visible ON app.records",
        "DROP ROW POLICY visible ON app.records CASCADE",
        "DROP CELL POLICY p ON app.t RESTRICT; DROP ROLE r",
    ] {
        assert!(parse_drop_policy(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_typed_add_column_with_bounded_defaults() {
    let parsed = parse_add_column(
        "ALTER TABLE app.events ADD COLUMN label VARCHAR(30) NOT NULL DEFAULT 'unknown';",
    )
    .unwrap();
    assert_eq!(parsed.schema, "app");
    assert_eq!(parsed.table, "events");
    assert!(!parsed.if_not_exists);
    assert_eq!(parsed.column.name, "label");
    assert_eq!(
        parsed.column.scalar_type,
        ScalarType::Varchar { max_chars: 30 }
    );
    assert!(!parsed.column.nullable);
    assert_eq!(
        parsed.column.default,
        Some(AdminColumnDefault::Literal(ScalarValue::Varchar(
            "unknown".into()
        )))
    );
    let conditional =
        parse_add_column("ALTER TABLE app.events ADD COLUMN IF NOT EXISTS label TEXT").unwrap();
    assert!(conditional.if_not_exists);
    for sql in [
        "ALTER TABLE app.events ADD COLUMN label TEXT NOT NULL",
        "ALTER TABLE app.events ADD COLUMN label TEXT DEFAULT CURRENT_PRINCIPAL_ID()",
        "ALTER TABLE app.events ADD COLUMN label TEXT NOT NULL DEFAULT NULL",
        "ALTER TABLE events ADD COLUMN label TEXT",
        "ALTER TABLE app.events ADD COLUMN label TEXT; ALTER TABLE app.events ADD COLUMN extra TEXT",
    ] {
        assert!(parse_add_column(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_qualified_column_rename_only() {
    assert_eq!(
        parse_rename_column("ALTER TABLE app.events RENAME COLUMN old_name TO new_name;").unwrap(),
        RenameColumnSql {
            schema: "app".into(),
            table: "events".into(),
            column: "old_name".into(),
            new_name: "new_name".into()
        }
    );
    for sql in [
        "ALTER TABLE events RENAME COLUMN old_name TO new_name",
        "ALTER TABLE app.events RENAME old_name TO new_name",
        "ALTER TABLE app.events RENAME COLUMN old_name new_name",
        "ALTER TABLE app.events RENAME COLUMN old_name TO new_name; ALTER TABLE app.events RENAME COLUMN x TO y",
    ] {
        assert!(parse_rename_column(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_restricted_column_drop_only() {
    assert_eq!(
        parse_drop_column("ALTER TABLE app.events DROP COLUMN obsolete RESTRICT;").unwrap(),
        DropColumnSql {
            schema: "app".into(),
            table: "events".into(),
            column: "obsolete".into(),
            if_exists: false,
        }
    );
    for sql in [
        "ALTER TABLE events DROP COLUMN obsolete RESTRICT",
        "ALTER TABLE app.events DROP obsolete RESTRICT",
        "ALTER TABLE app.events DROP COLUMN obsolete CASCADE",
        "ALTER TABLE app.events DROP COLUMN obsolete RESTRICT; ALTER TABLE app.events DROP COLUMN other RESTRICT",
    ] {
        assert!(parse_drop_column(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_composite_primary_key_and_implies_not_null() {
    let parsed = parse_create_table(
            "CREATE TABLE app.events (tenant UUID, event_id BIGINT, label TEXT, PRIMARY KEY (tenant, event_id))",
        )
        .unwrap();
    assert_eq!(
        parsed.primary_key_columns,
        Some(vec!["tenant".into(), "event_id".into()])
    );
    assert!(!parsed.columns[0].nullable);
    assert!(!parsed.columns[1].nullable);
    assert!(parsed.columns[2].nullable);
    for sql in [
        "CREATE TABLE app.t (id UUID, PRIMARY KEY (missing))",
        "CREATE TABLE app.t (id UUID, PRIMARY KEY (id, id))",
        "CREATE TABLE app.t (id UUID, PRIMARY KEY (id), extra INTEGER)",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_table_level_foreign_key_constraints() {
    let parsed = parse_create_table(
            "CREATE TABLE app.order_lines (customer_id INTEGER, order_id BIGINT, FOREIGN KEY (customer_id) REFERENCES crm.customers (id), FOREIGN KEY (order_id) REFERENCES app.orders (id));",
        )
        .unwrap();
    assert_eq!(
        parsed.foreign_keys,
        vec![
            CreateTableForeignKeySql {
                name: None,
                columns: vec!["customer_id".into()],
                referenced_schema: "crm".into(),
                referenced_table: "customers".into(),
                referenced_columns: vec!["id".into()],
            },
            CreateTableForeignKeySql {
                name: None,
                columns: vec!["order_id".into()],
                referenced_schema: "app".into(),
                referenced_table: "orders".into(),
                referenced_columns: vec!["id".into()],
            },
        ]
    );
    for sql in [
        "CREATE TABLE app.order_lines (customer_id INTEGER, FOREIGN KEY (missing) REFERENCES crm.customers (id))",
        "CREATE TABLE app.order_lines (customer_id INTEGER, FOREIGN KEY (customer_id, customer_id) REFERENCES crm.customers (id, other))",
        "CREATE TABLE app.order_lines (customer_id INTEGER, FOREIGN KEY (customer_id) REFERENCES customers (id))",
        "CREATE TABLE app.order_lines (customer_id INTEGER, FOREIGN KEY (customer_id) REFERENCES crm.customers (id)); CREATE TABLE app.x (id INTEGER)",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_column_level_foreign_key_constraints() {
    let parsed = parse_create_table(
            "CREATE TABLE app.order_lines (customer_id INTEGER REFERENCES crm.customers (id), order_id BIGINT, FOREIGN KEY (order_id) REFERENCES app.orders (id));",
        )
        .unwrap();
    assert_eq!(
        parsed.foreign_keys,
        vec![
            CreateTableForeignKeySql {
                name: None,
                columns: vec!["customer_id".into()],
                referenced_schema: "crm".into(),
                referenced_table: "customers".into(),
                referenced_columns: vec!["id".into()],
            },
            CreateTableForeignKeySql {
                name: None,
                columns: vec!["order_id".into()],
                referenced_schema: "app".into(),
                referenced_table: "orders".into(),
                referenced_columns: vec!["id".into()],
            },
        ]
    );
    for sql in [
        "CREATE TABLE app.order_lines (customer_id INTEGER REFERENCES customers (id))",
        "CREATE TABLE app.order_lines (customer_id INTEGER REFERENCES crm.customers (id, region_id))",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_multiple_table_unique_constraints() {
    let parsed = parse_create_table(
            "CREATE TABLE app.events (id UUID NOT NULL, email TEXT, tenant UUID, PRIMARY KEY (id), UNIQUE (email), UNIQUE (tenant, email));",
        )
        .unwrap();
    assert_eq!(parsed.primary_key_columns, Some(vec!["id".into()]));
    assert_eq!(
        parsed.unique_constraints,
        vec![
            vec!["email".to_owned()],
            vec!["tenant".to_owned(), "email".to_owned()],
        ]
    );
    assert!(!parsed.columns[0].nullable);
    assert!(parsed.columns[1].nullable);
    for sql in [
        "CREATE TABLE app.events (id UUID, UNIQUE (missing))",
        "CREATE TABLE app.events (id UUID, UNIQUE (id, id))",
        "CREATE TABLE app.events (id UUID, UNIQUE (id), UNIQUE (id))",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn rejects_unsupported_or_ambiguous_ddl() {
    for sql in [
        "CREATE TABLE events (id UUID)",
        "CREATE TABLE app.events (id UUID, other INTEGER CHECK (id IS NOT NULL))",
        "CREATE TABLE app.events (id UUID); CREATE TABLE app.other (id UUID)",
        "CREATE TABLE \"app\".events (id UUID)",
        "CREATE TABLE app.events (id DECIMAL(39, 0))",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_exact_typed_defaults_and_modifier_orders() {
    for column in [
        "label TEXT DEFAULT 'O''Reilly'",
        "amount DECIMAL(6,2) DEFAULT -12.30 NOT NULL",
        "active BOOLEAN NOT NULL DEFAULT TRUE",
        "raw BYTEA DEFAULT X'00ff'",
        "owner UUID DEFAULT CURRENT_PRINCIPAL_ID()",
    ] {
        parse_create_table(&format!("CREATE TABLE app.values ({column})"))
            .unwrap_or_else(|error| panic!("{column}: {error:?}"));
    }
    let parsed = parse_create_table(
            "CREATE TABLE app.values (label TEXT DEFAULT 'O''Reilly', amount DECIMAL(6,2) DEFAULT -12.30 NOT NULL, active BOOLEAN NOT NULL DEFAULT TRUE, raw BYTEA DEFAULT X'00FF', owner UUID DEFAULT CURRENT_PRINCIPAL_ID(), fixed_id UUID DEFAULT '00112233-4455-6677-8899-aabbccddeeff', optional TEXT DEFAULT NULL);",
        )
        .unwrap();
    assert_eq!(
        parsed.columns[0].default,
        Some(AdminColumnDefault::Literal(ScalarValue::Text(
            "O'Reilly".into()
        )))
    );
    assert_eq!(
        parsed.columns[1].default,
        Some(AdminColumnDefault::Literal(ScalarValue::Decimal(-1230)))
    );
    assert!(!parsed.columns[1].nullable);
    assert!(!parsed.columns[2].nullable);
    assert_eq!(
        parsed.columns[2].default,
        Some(AdminColumnDefault::Literal(ScalarValue::Boolean(true)))
    );
    assert_eq!(
        parsed.columns[3].default,
        Some(AdminColumnDefault::Literal(ScalarValue::Bytea(vec![
            0, 255
        ])))
    );
    assert_eq!(
        parsed.columns[4].default,
        Some(AdminColumnDefault::CurrentPrincipalId)
    );
    assert_eq!(
        parsed.columns[5].default,
        Some(AdminColumnDefault::Literal(ScalarValue::Uuid([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ])))
    );
    assert_eq!(parsed.columns[6].default, Some(AdminColumnDefault::Null));
}

#[test]
fn parses_storage_generated_uuid_identity_and_rejects_conflicts() {
    let parsed = parse_create_table(
            "CREATE TABLE app.events (id UUID GENERATED ALWAYS AS IDENTITY, other UUID GENERATED ALWAYS AS IDENTITY NOT NULL)",
        )
        .unwrap();
    assert!(parsed.columns.iter().all(|column| {
        column.generated_uuid_identity && !column.nullable && column.default.is_none()
    }));

    for sql in [
        "CREATE TABLE app.t (id INTEGER GENERATED ALWAYS AS IDENTITY)",
        "CREATE TABLE app.t (id UUID GENERATED ALWAYS AS IDENTITY DEFAULT NULL)",
        "CREATE TABLE app.t (id UUID GENERATED AS IDENTITY)",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn rejects_defaults_with_loss_or_type_mismatch() {
    for sql in [
        "CREATE TABLE app.t (n DECIMAL(4,2) DEFAULT 1.234)",
        "CREATE TABLE app.t (n DECIMAL(2,1) DEFAULT 123.0)",
        "CREATE TABLE app.t (n INTEGER DEFAULT '1')",
        "CREATE TABLE app.t (n BYTEA DEFAULT X'0')",
        "CREATE TABLE app.t (n INTEGER DEFAULT CURRENT_PRINCIPAL_ID())",
        "CREATE TABLE app.t (n VARCHAR(2) DEFAULT 'abc')",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}
