use super::*;
use crate::client_submit::AdminColumnDefault;
use crate::tuple::{ScalarType, ScalarValue};

#[test]
fn parses_qualified_create_table_with_supported_types() {
    let parsed = parse_create_table(
        "CREATE TABLE app.events (id UUID NOT NULL, amount DECIMAL(8, 2), title VARCHAR(80));",
    )
    .unwrap();
    assert_eq!(parsed.schema, "app");
    assert_eq!(parsed.table, "events");
    assert_eq!(parsed.columns.len(), 3);
    assert!(!parsed.columns[0].nullable);
    assert_eq!(
        parsed.columns[1].scalar_type,
        ScalarType::Decimal {
            precision: 8,
            scale: 2
        }
    );
    assert_eq!(
        parsed.columns[2].scalar_type,
        ScalarType::Varchar { max_chars: 80 }
    );
}

#[test]
fn parses_create_schema_with_explicit_uuid_owner() {
    let parsed = parse_create_schema(
        "CREATE SCHEMA app AUTHORIZATION '00112233-4455-6677-8899-aabbccddeeff';",
    )
    .unwrap();
    assert_eq!(parsed.schema, "app");
    assert_eq!(
        parsed.owner,
        CreateSchemaOwnerSql::Id([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ])
    );
    assert_eq!(
        parse_create_schema("CREATE SCHEMA app;").unwrap().owner,
        CreateSchemaOwnerSql::CurrentUser
    );
    let conditional =
        parse_create_schema("CREATE SCHEMA IF NOT EXISTS app AUTHORIZATION ROLE app_admin;")
            .unwrap();
    assert!(conditional.if_not_exists);
    assert_eq!(conditional.schema, "app");
    assert_eq!(
        conditional.owner,
        CreateSchemaOwnerSql::Identity {
            name: "app_admin".into(),
            kind: SqlIdentityKind::Role,
        }
    );
    assert_eq!(
        parse_create_schema("CREATE SCHEMA app AUTHORIZATION USER app_owner")
            .unwrap()
            .owner,
        CreateSchemaOwnerSql::Identity {
            name: "app_owner".into(),
            kind: SqlIdentityKind::User,
        }
    );
    assert_eq!(
        parse_create_schema("CREATE SCHEMA app AUTHORIZATION ROLE app_admin")
            .unwrap()
            .owner,
        CreateSchemaOwnerSql::Identity {
            name: "app_admin".into(),
            kind: SqlIdentityKind::Role,
        }
    );
    assert_eq!(
        parse_create_schema("CREATE SCHEMA app AUTHORIZATION app_owner")
            .unwrap()
            .owner,
        CreateSchemaOwnerSql::Identity {
            name: "app_owner".into(),
            kind: SqlIdentityKind::Either,
        }
    );
    for sql in [
        "CREATE SCHEMA app AUTHORIZATION GROUP app_owner",
        "CREATE SCHEMA app AUTHORIZATION '00000000-0000-0000-0000-000000000000'",
        "CREATE SCHEMA app AUTHORIZATION '00112233445566778899aabbccddeeff'; CREATE SCHEMA b AUTHORIZATION '00112233445566778899aabbccddeeff'",
    ] {
        assert!(parse_create_schema(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_schema_rename_and_restrict_drop_only() {
    assert_eq!(
        parse_rename_schema("ALTER SCHEMA app RENAME TO application;").unwrap(),
        RenameSchemaSql {
            schema: "app".into(),
            new_name: "application".into(),
        }
    );
    assert_eq!(
        parse_drop_schema("DROP SCHEMA app RESTRICT").unwrap(),
        DropSchemaSql {
            schema: "app".into(),
            if_exists: false,
        }
    );
    assert_eq!(
        parse_drop_schema("DROP SCHEMA IF EXISTS app RESTRICT;").unwrap(),
        DropSchemaSql {
            schema: "app".into(),
            if_exists: true,
        }
    );
    for sql in [
        "ALTER SCHEMA app RENAME application",
        "ALTER SCHEMA app RENAME TO \"Mixed\"",
        "DROP SCHEMA app",
        "DROP SCHEMA app CASCADE",
        "DROP SCHEMA IF app RESTRICT",
        "DROP SCHEMA IF NOT EXISTS app RESTRICT",
        "DROP SCHEMA app RESTRICT; DROP SCHEMA b RESTRICT",
    ] {
        assert!(
            parse_rename_schema(sql).is_err() && parse_drop_schema(sql).is_err(),
            "accepted {sql}"
        );
    }
    assert_eq!(
        parse_schema_owner_transfer("ALTER SCHEMA app OWNER TO USER owner_name").unwrap(),
        OwnerTransferSql {
            schema: "app".into(),
            table: None,
            owner_name: "owner_name".into(),
            owner_kind: SqlIdentityKind::User,
        }
    );
    assert_eq!(
        parse_schema_owner_transfer("ALTER SCHEMA app OWNER TO owner_name").unwrap(),
        OwnerTransferSql {
            schema: "app".into(),
            table: None,
            owner_name: "owner_name".into(),
            owner_kind: SqlIdentityKind::Either,
        }
    );
    let invalid = "ALTER SCHEMA app OWNER TO USER owner_name CASCADE";
    assert!(
        parse_schema_owner_transfer(invalid).is_err(),
        "accepted {invalid}"
    );
}

#[test]
fn parses_qualified_table_rename_and_restrict_drop() {
    assert_eq!(
        parse_rename_table("ALTER TABLE app.events RENAME TO archived;").unwrap(),
        RenameTableSql {
            schema: "app".into(),
            table: "events".into(),
            new_name: "archived".into(),
        }
    );
    assert_eq!(
        parse_drop_table("DROP TABLE app.events RESTRICT").unwrap(),
        DropTableSql {
            schema: "app".into(),
            table: "events".into(),
            if_exists: false,
        }
    );
    assert_eq!(
        parse_drop_table("DROP TABLE IF EXISTS app.events RESTRICT;").unwrap(),
        DropTableSql {
            schema: "app".into(),
            table: "events".into(),
            if_exists: true,
        }
    );
    assert_eq!(
        parse_truncate_table("TRUNCATE TABLE app.events RESTRICT;").unwrap(),
        TruncateTableSql {
            schema: "app".into(),
            table: "events".into(),
        }
    );
    for sql in [
        "ALTER TABLE events RENAME TO archived",
        "ALTER TABLE app.events RENAME archived",
        "DROP TABLE app.events",
        "DROP TABLE app.events CASCADE",
        "DROP TABLE IF app.events RESTRICT",
        "DROP TABLE IF NOT EXISTS app.events RESTRICT",
        "DROP TABLE app.events RESTRICT; DROP TABLE app.other RESTRICT",
    ] {
        assert!(
            parse_rename_table(sql).is_err() && parse_drop_table(sql).is_err(),
            "accepted {sql}"
        );
    }
    for sql in [
        "TRUNCATE app.events RESTRICT",
        "TRUNCATE TABLE events RESTRICT",
        "TRUNCATE TABLE app.events",
        "TRUNCATE TABLE app.events CASCADE",
        "TRUNCATE TABLE app.events RESTRICT; TRUNCATE TABLE app.other RESTRICT",
    ] {
        assert!(parse_truncate_table(sql).is_err(), "accepted {sql}");
    }
    assert_eq!(
        parse_table_owner_transfer("ALTER TABLE app.events OWNER TO ROLE owners;").unwrap(),
        OwnerTransferSql {
            schema: "app".into(),
            table: Some("events".into()),
            owner_name: "owners".into(),
            owner_kind: SqlIdentityKind::Role,
        }
    );
    for sql in [
        "ALTER TABLE app.events OWNER TO owners",
        "ALTER TABLE events OWNER TO ROLE owners",
        "ALTER TABLE app.events OWNER TO ROLE owners CASCADE",
    ] {
        assert!(parse_table_owner_transfer(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_table_level_key_constraint_alterations() {
    assert_eq!(
        parse_add_key_constraint("ALTER TABLE app.events ADD UNIQUE (tenant, event_id);").unwrap(),
        AddKeyConstraintSql {
            schema: "app".into(),
            table: "events".into(),
            name: None,
            columns: vec!["tenant".into(), "event_id".into()],
            kind: KeyConstraintKind::Unique,
        }
    );
    assert_eq!(
        parse_add_key_constraint("ALTER TABLE app.events ADD PRIMARY KEY (id)")
            .unwrap()
            .kind,
        KeyConstraintKind::PrimaryKey
    );
    assert_eq!(
        parse_add_key_constraint("ALTER TABLE app.events ADD CONSTRAINT events_key UNIQUE (id)")
            .unwrap()
            .name
            .as_deref(),
        Some("events_key")
    );
    for sql in [
        "ALTER TABLE events ADD UNIQUE (id)",
        "ALTER TABLE app.events ADD UNIQUE ()",
        "ALTER TABLE app.events ADD UNIQUE (id, id)",
        "ALTER TABLE app.events ADD UNIQUE (id); ALTER TABLE app.events ADD UNIQUE (id)",
    ] {
        assert!(parse_add_key_constraint(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_qualified_foreign_key_alteration() {
    assert_eq!(
            parse_add_foreign_key(
                "ALTER TABLE app.events ADD FOREIGN KEY (tenant_id, event_id) REFERENCES core.tenants (tenant_id, id);"
            )
            .unwrap(),
            AddForeignKeySql {
                schema: "app".into(),
                table: "events".into(),
                name: None,
                columns: vec!["tenant_id".into(), "event_id".into()],
                referenced_schema: "core".into(),
                referenced_table: "tenants".into(),
                referenced_columns: vec!["tenant_id".into(), "id".into()],
            }
        );
    assert_eq!(
            parse_add_foreign_key(
                "ALTER TABLE app.events ADD CONSTRAINT events_tenant_fk FOREIGN KEY (tenant_id) REFERENCES core.tenants (id)"
            )
            .unwrap()
            .name
            .as_deref(),
            Some("events_tenant_fk")
        );
    for sql in [
        "ALTER TABLE app.events ADD FOREIGN KEY (tenant_id) REFERENCES core.tenants (tenant_id, id)",
        "ALTER TABLE app.events ADD FOREIGN KEY (tenant_id, tenant_id) REFERENCES core.tenants (tenant_id, id)",
        "ALTER TABLE app.events ADD FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id)",
        "ALTER TABLE app.events ADD FOREIGN KEY (tenant_id) REFERENCES core.tenants (tenant_id); ALTER TABLE app.events ADD FOREIGN KEY (x) REFERENCES core.tenants (x)",
    ] {
        assert!(parse_add_foreign_key(sql).is_err(), "accepted {sql}");
    }
    assert_eq!(
        parse_drop_constraint(
            "ALTER TABLE app.orders DROP CONSTRAINT '01234567-89ab-cdef-0123-456789abcdef'"
        )
        .unwrap(),
        DropConstraintSql {
            schema: "app".into(),
            table: "orders".into(),
            constraint_id: [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
                0xcd, 0xef,
            ],
            constraint_name: None,
            if_exists: false,
        }
    );
    assert!(parse_drop_constraint(
            "ALTER TABLE app.orders DROP CONSTRAINT IF EXISTS '01234567-89ab-cdef-0123-456789abcdef'"
        )
        .unwrap()
        .if_exists);
    for sql in [
        "ALTER TABLE orders DROP CONSTRAINT '0123456789abcdef0123456789abcdef'",
        "ALTER TABLE app.orders DROP CONSTRAINT '00000000000000000000000000000000'",
    ] {
        assert!(parse_drop_constraint(sql).is_err(), "accepted {sql}");
    }
    let named = parse_drop_constraint(
        "ALTER TABLE app.orders DROP CONSTRAINT IF EXISTS fk_orders_customer",
    )
    .unwrap();
    assert_eq!(named.constraint_name.as_deref(), Some("fk_orders_customer"));
    assert!(named.if_exists);
}

#[test]
fn parses_table_level_check_constraints_and_binds_column_names() {
    let parsed = parse_create_table(
            "CREATE TABLE app.events (id UUID NOT NULL, active BOOLEAN, deleted_at TIMESTAMPTZ, amount DECIMAL(8,2), PRIMARY KEY (id), CHECK (active OR deleted_at IS NULL), CHECK (amount >= -1.25));",
        )
        .unwrap();
    assert_eq!(
        parsed.check_constraints,
        vec![
            CheckExpressionSql::Or(vec![
                CheckExpressionSql::Column("active".into()),
                CheckExpressionSql::IsNull(Box::new(CheckExpressionSql::Column(
                    "deleted_at".into(),
                ))),
            ]),
            CheckExpressionSql::Compare {
                operator: CheckComparisonOperator::GreaterEqual,
                left: Box::new(CheckExpressionSql::Column("amount".into())),
                right: Box::new(CheckExpressionSql::Literal(CheckLiteralSql::Number(
                    "-1.25".into(),
                ))),
            },
        ]
    );
    for sql in [
        "CREATE TABLE app.events (id INTEGER, CHECK (missing IS NULL))",
        "CREATE TABLE app.events (id INTEGER, CHECK (id))",
        "CREATE TABLE app.events (amount DECIMAL(8,2), CHECK (amount >= 0.001))",
        "CREATE TABLE app.events (amount TEXT, CHECK (amount >= 0))",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_named_create_table_check_constraints() {
    let parsed = parse_create_table(
            "CREATE TABLE app.events (id INTEGER, label TEXT CONSTRAINT label_nonempty CHECK (label IS NOT NULL), CONSTRAINT id_positive CHECK (id > 0));",
        )
        .unwrap();
    assert_eq!(
        parsed.check_constraint_names,
        vec![Some("label_nonempty".into()), Some("id_positive".into())]
    );
    assert_eq!(parsed.check_constraints.len(), 2);
    for sql in [
        "CREATE TABLE app.events (id INTEGER, CONSTRAINT repeated CHECK (id > 0), CONSTRAINT repeated CHECK (id < 10))",
        "CREATE TABLE app.events (id INTEGER, CONSTRAINT 9bad CHECK (id > 0))",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_named_create_table_key_and_foreign_key_constraints() {
    let parsed = parse_create_table(
            "CREATE TABLE app.orders (id UUID CONSTRAINT orders_pk PRIMARY KEY, code TEXT CONSTRAINT orders_code_key UNIQUE, customer_id UUID, CONSTRAINT customer_fk FOREIGN KEY (customer_id) REFERENCES crm.customers (id), CONSTRAINT other_key UNIQUE (code, customer_id))",
        )
        .unwrap();
    assert_eq!(parsed.primary_key_name.as_deref(), Some("orders_pk"));
    assert_eq!(
        parsed.unique_constraint_names,
        vec![Some("orders_code_key".into()), Some("other_key".into())]
    );
    assert_eq!(parsed.foreign_key_names, vec![Some("customer_fk".into())]);
    assert_eq!(parsed.foreign_keys[0].name.as_deref(), Some("customer_fk"));

    for sql in [
        "CREATE TABLE app.t (id UUID CONSTRAINT same PRIMARY KEY, value TEXT CONSTRAINT same UNIQUE)",
        "CREATE TABLE app.t (id UUID, CONSTRAINT same UNIQUE (id), CONSTRAINT same FOREIGN KEY (id) REFERENCES app.x (id))",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_create_table_if_not_exists() {
    let parsed =
        parse_create_table("CREATE TABLE IF NOT EXISTS app.events (id UUID PRIMARY KEY);").unwrap();
    assert!(parsed.if_not_exists);
    assert_eq!(parsed.schema, "app");
    assert_eq!(parsed.table, "events");
    assert_eq!(parsed.primary_key_columns, Some(vec!["id".into()]));
    assert!(
        !parse_create_table("CREATE TABLE app.events (id UUID)")
            .unwrap()
            .if_not_exists
    );
}

#[test]
fn parses_column_level_unique_as_a_single_column_constraint() {
    let parsed = parse_create_table(
            "CREATE TABLE app.accounts (id UUID GENERATED ALWAYS AS IDENTITY, email TEXT UNIQUE NOT NULL, nickname TEXT DEFAULT 'guest', PRIMARY KEY (id));",
        )
        .unwrap();
    assert_eq!(parsed.primary_key_columns, Some(vec!["id".into()]));
    assert_eq!(parsed.unique_constraints, vec![vec![String::from("email")]]);
    assert!(!parsed.columns[1].nullable);
    assert_eq!(
        parsed.columns[2].default,
        Some(AdminColumnDefault::Literal(ScalarValue::Text(
            "guest".into()
        )))
    );

    for sql in [
        "CREATE TABLE app.accounts (email TEXT UNIQUE UNIQUE)",
        "CREATE TABLE app.accounts (email TEXT UNIQUE, UNIQUE (email))",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_column_level_primary_key_as_non_nullable() {
    let parsed =
        parse_create_table("CREATE TABLE app.accounts (id UUID PRIMARY KEY, email TEXT UNIQUE);")
            .unwrap();
    assert_eq!(parsed.primary_key_columns, Some(vec![String::from("id")]));
    assert!(!parsed.columns[0].nullable);
    assert_eq!(parsed.unique_constraints, vec![vec![String::from("email")]]);

    for sql in [
        "CREATE TABLE app.accounts (id INTEGER PRIMARY KEY, email INTEGER PRIMARY KEY)",
        "CREATE TABLE app.accounts (id INTEGER PRIMARY KEY, PRIMARY KEY (id))",
        "CREATE TABLE app.accounts (id INTEGER PRIMARY KEY PRIMARY KEY)",
    ] {
        assert!(parse_create_table(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_column_level_check_against_its_declared_column() {
    let parsed =
        parse_create_table("CREATE TABLE app.payments (amount DECIMAL(8,2) CHECK (amount >= 0));")
            .unwrap();
    assert_eq!(
        parsed.check_constraints,
        vec![CheckExpressionSql::Compare {
            operator: CheckComparisonOperator::GreaterEqual,
            left: Box::new(CheckExpressionSql::Column("amount".into())),
            right: Box::new(CheckExpressionSql::Literal(CheckLiteralSql::Number(
                "0".into()
            ))),
        }]
    );
    assert!(
        parse_create_table(
            "CREATE TABLE app.payments (amount DECIMAL(8,2), other INTEGER, CHECK (amount >= 0));"
        )
        .is_ok()
    );
    assert!(
        parse_create_table(
            "CREATE TABLE app.payments (amount DECIMAL(8,2) CHECK (other >= 0), other INTEGER)"
        )
        .is_err()
    );
}

#[test]
fn binds_check_comparison_literals_to_column_types() {
    use crate::authorization::Expr;

    let expression = CheckExpressionSql::Compare {
        operator: CheckComparisonOperator::GreaterEqual,
        left: Box::new(CheckExpressionSql::Column("amount".into())),
        right: Box::new(CheckExpressionSql::Literal(CheckLiteralSql::Number(
            "-1.25".into(),
        ))),
    };
    let bound = bind_check_expression(&expression, |name| {
        (name == "amount").then_some((
            0,
            ScalarType::Decimal {
                precision: 8,
                scale: 2,
            },
        ))
    })
    .unwrap();
    assert_eq!(
        bound,
        Expr::GreaterEqual(
            Box::new(Expr::Field(0)),
            Box::new(Expr::Literal(Some(ScalarValue::Decimal(-125)))),
        )
    );

    let grouped =
        parse_add_check_constraint("ALTER TABLE app.events ADD CHECK (enabled = (priority >= 1))")
            .unwrap();
    assert_eq!(
        bind_check_expression(&grouped.expression, |name| match name {
            "enabled" => Some((0, ScalarType::Boolean)),
            "priority" => Some((1, ScalarType::Integer)),
            _ => None,
        })
        .unwrap(),
        Expr::Equal(
            Box::new(Expr::Field(0)),
            Box::new(Expr::GreaterEqual(
                Box::new(Expr::Field(1)),
                Box::new(Expr::Literal(Some(ScalarValue::Integer(1)))),
            )),
        )
    );

    let string_check =
        parse_create_table("CREATE TABLE app.events (label VARCHAR(8), CHECK (label = 'ready'))")
            .unwrap();
    assert_eq!(string_check.check_constraints.len(), 1);
    assert!(
        parse_create_table("CREATE TABLE app.events (active BOOLEAN, CHECK (active = 'ready'))")
            .is_err()
    );
    assert!(
        parse_create_table("CREATE TABLE app.events (amount INTEGER, CHECK (amount >= '0'))")
            .is_err()
    );

    for (operator, expected) in [
        ("=", CheckComparisonOperator::Equal),
        ("!=", CheckComparisonOperator::NotEqual),
        ("<>", CheckComparisonOperator::NotEqual),
        ("<", CheckComparisonOperator::Less),
        ("<=", CheckComparisonOperator::LessEqual),
        (">", CheckComparisonOperator::Greater),
        (">=", CheckComparisonOperator::GreaterEqual),
    ] {
        let statement = format!("ALTER TABLE app.events ADD CHECK (amount {operator} 0)");
        assert!(matches!(
            parse_add_check_constraint(&statement).unwrap().expression,
            CheckExpressionSql::Compare { operator, .. } if operator == expected
        ));
    }
}

#[test]
fn parses_and_binds_integer_check_arithmetic_with_precedence() {
    use crate::authorization::Expr;

    let parsed = parse_add_check_constraint(
        "ALTER TABLE app.inventory ADD CHECK (quantity * 2 + reserved <= capacity)",
    )
    .unwrap();
    assert_eq!(
        parsed.expression,
        CheckExpressionSql::Compare {
            operator: CheckComparisonOperator::LessEqual,
            left: Box::new(CheckExpressionSql::Add(
                Box::new(CheckExpressionSql::Multiply(
                    Box::new(CheckExpressionSql::Column("quantity".into())),
                    Box::new(CheckExpressionSql::Literal(CheckLiteralSql::Number(
                        "2".into(),
                    ))),
                )),
                Box::new(CheckExpressionSql::Column("reserved".into())),
            )),
            right: Box::new(CheckExpressionSql::Column("capacity".into())),
        }
    );
    let bound = bind_check_expression(&parsed.expression, |name| match name {
        "quantity" => Some((0, ScalarType::Integer)),
        "reserved" => Some((1, ScalarType::Integer)),
        "capacity" => Some((2, ScalarType::Integer)),
        _ => None,
    })
    .unwrap();
    assert_eq!(
        bound,
        Expr::LessEqual(
            Box::new(Expr::Add(
                Box::new(Expr::Multiply(
                    Box::new(Expr::Field(0)),
                    Box::new(Expr::Literal(Some(ScalarValue::Integer(2)))),
                )),
                Box::new(Expr::Field(1)),
            )),
            Box::new(Expr::Field(2)),
        )
    );
    let modulo =
        parse_add_check_constraint("ALTER TABLE app.inventory ADD CHECK (quantity % 2 = 0)")
            .unwrap();
    assert_eq!(
        bind_check_expression(&modulo.expression, |name| {
            (name == "quantity").then_some((0, ScalarType::Integer))
        })
        .unwrap(),
        Expr::Equal(
            Box::new(Expr::Modulo(
                Box::new(Expr::Field(0)),
                Box::new(Expr::Literal(Some(ScalarValue::Integer(2)))),
            )),
            Box::new(Expr::Literal(Some(ScalarValue::Integer(0)))),
        )
    );
    let divide = parse_add_check_constraint(
        "ALTER TABLE app.inventory ADD CHECK (quantity / 2 * reserved = capacity)",
    )
    .unwrap();
    assert_eq!(
        divide.expression,
        CheckExpressionSql::Compare {
            operator: CheckComparisonOperator::Equal,
            left: Box::new(CheckExpressionSql::Multiply(
                Box::new(CheckExpressionSql::Divide(
                    Box::new(CheckExpressionSql::Column("quantity".into())),
                    Box::new(CheckExpressionSql::Literal(CheckLiteralSql::Number(
                        "2".into(),
                    ))),
                )),
                Box::new(CheckExpressionSql::Column("reserved".into())),
            )),
            right: Box::new(CheckExpressionSql::Column("capacity".into())),
        }
    );
    assert!(
        bind_check_expression(&divide.expression, |name| match name {
            "quantity" | "reserved" | "capacity" => Some((0, ScalarType::Integer)),
            _ => None,
        })
        .is_ok()
    );
    let negation =
        parse_add_check_constraint("ALTER TABLE app.inventory ADD CHECK (-quantity <= 0)").unwrap();
    assert_eq!(
        bind_check_expression(&negation.expression, |name| {
            (name == "quantity").then_some((0, ScalarType::Integer))
        })
        .unwrap(),
        Expr::LessEqual(
            Box::new(Expr::Negate(Box::new(Expr::Field(0)))),
            Box::new(Expr::Literal(Some(ScalarValue::Integer(0)))),
        )
    );
    let null_negation =
        parse_add_check_constraint("ALTER TABLE app.inventory ADD CHECK (-NULL IS NULL)").unwrap();
    assert!(bind_check_expression(&null_negation.expression, |_| None).is_ok());
    assert!(
        parse_add_check_constraint(
            "ALTER TABLE app.inventory ADD CHECK (quantity + 'two' <= capacity)"
        )
        .is_ok()
    );
    assert!(
        bind_check_expression(
            &parse_add_check_constraint(
                "ALTER TABLE app.inventory ADD CHECK (quantity + 'two' <= capacity)"
            )
            .unwrap()
            .expression,
            |name| match name {
                "quantity" | "capacity" => Some((0, ScalarType::Integer)),
                _ => None,
            }
        )
        .is_err()
    );
}

#[test]
fn parses_and_binds_check_in_predicates_with_typed_literals() {
    use crate::authorization::Expr;

    let parsed = parse_create_table(
        "CREATE TABLE app.jobs (state VARCHAR(8), CHECK (state IN ('new', 'running', 'done')))",
    )
    .unwrap();
    let expected = CheckExpressionSql::In {
        expression: Box::new(CheckExpressionSql::Column("state".into())),
        values: vec![
            Box::new(CheckExpressionSql::Literal(CheckLiteralSql::String(
                "new".into(),
            ))),
            Box::new(CheckExpressionSql::Literal(CheckLiteralSql::String(
                "running".into(),
            ))),
            Box::new(CheckExpressionSql::Literal(CheckLiteralSql::String(
                "done".into(),
            ))),
        ],
        negated: false,
    };
    assert_eq!(parsed.check_constraints, vec![expected.clone()]);
    assert_eq!(
        bind_check_expression(&expected, |name| {
            (name == "state").then_some((0, ScalarType::Varchar { max_chars: 8 }))
        })
        .unwrap(),
        Expr::Or(vec![
            Expr::Equal(
                Box::new(Expr::Field(0)),
                Box::new(Expr::Literal(Some(ScalarValue::Varchar("new".into())))),
            ),
            Expr::Equal(
                Box::new(Expr::Field(0)),
                Box::new(Expr::Literal(Some(ScalarValue::Varchar("running".into())))),
            ),
            Expr::Equal(
                Box::new(Expr::Field(0)),
                Box::new(Expr::Literal(Some(ScalarValue::Varchar("done".into())))),
            ),
        ])
    );

    let negative =
        parse_add_check_constraint("ALTER TABLE app.jobs ADD CHECK (priority NOT IN (1, 2, NULL))")
            .unwrap();
    assert!(matches!(
        negative.expression,
        CheckExpressionSql::In {
            ref expression,
            ref values,
            negated: true
        } if expression.as_ref() == &CheckExpressionSql::Column("priority".into()) && values.len() == 3
    ));
    assert!(parse_create_table("CREATE TABLE app.jobs (state TEXT, CHECK (state IN ()))").is_err());
    let literal =
        parse_add_check_constraint("ALTER TABLE app.jobs ADD CHECK ('ok' IN ('ok'))").unwrap();
    assert!(bind_check_expression(&literal.expression, |_| None).is_ok());
    let grouped = parse_add_check_constraint(
        "ALTER TABLE app.jobs ADD CHECK ((priority >= 1) IN (TRUE, FALSE))",
    )
    .unwrap();
    assert!(
        bind_check_expression(&grouped.expression, |name| {
            (name == "priority").then_some((0, ScalarType::Integer))
        })
        .is_ok()
    );
    let columns = parse_create_table(
            "CREATE TABLE app.jobs (state TEXT, previous_state TEXT, CHECK (state IN ('new', previous_state)))",
        )
        .unwrap();
    assert!(
        bind_check_expression(&columns.check_constraints[0], |name| match name {
            "state" => Some((0, ScalarType::Text)),
            "previous_state" => Some((1, ScalarType::Text)),
            _ => None,
        })
        .is_ok()
    );
    let repeated_values = vec!["TRUE"; MAX_CHECK_IN_VALUES].join(", ");
    let oversized = parse_add_check_constraint(&format!(
        "ALTER TABLE app.jobs ADD CHECK ((priority >= 1) IN ({repeated_values}))"
    ))
    .unwrap();
    assert!(
        bind_check_expression(&oversized.expression, |name| {
            (name == "priority").then_some((0, ScalarType::Integer))
        })
        .is_err()
    );
    assert!(
        bind_check_expression(&negative.expression, |name| {
            (name == "priority").then_some((0, ScalarType::Integer))
        })
        .is_ok()
    );
    assert!(
        bind_check_expression(&negative.expression, |name| {
            (name == "priority").then_some((0, ScalarType::Boolean))
        })
        .is_err()
    );
    for count in [MAX_CHECK_IN_VALUES, MAX_CHECK_IN_VALUES + 1] {
        let values = (0..count)
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let sql =
            format!("CREATE TABLE app.jobs (priority INTEGER, CHECK (priority IN ({values})))");
        assert_eq!(
            parse_create_table(&sql).is_ok(),
            count == MAX_CHECK_IN_VALUES
        );
    }
}

#[test]
fn parses_and_binds_check_between_predicates_inclusively() {
    use crate::authorization::Expr;

    let parsed =
        parse_add_check_constraint("ALTER TABLE app.jobs ADD CHECK (priority BETWEEN 1 AND 9)")
            .unwrap();
    assert_eq!(
        parsed.expression,
        CheckExpressionSql::Between {
            expression: Box::new(CheckExpressionSql::Column("priority".into())),
            lower: Box::new(CheckExpressionSql::Literal(CheckLiteralSql::Number(
                "1".into(),
            ))),
            upper: Box::new(CheckExpressionSql::Literal(CheckLiteralSql::Number(
                "9".into(),
            ))),
            negated: false,
        }
    );
    assert_eq!(
        bind_check_expression(&parsed.expression, |name| {
            (name == "priority").then_some((0, ScalarType::Integer))
        })
        .unwrap(),
        Expr::And(vec![
            Expr::GreaterEqual(
                Box::new(Expr::Field(0)),
                Box::new(Expr::Literal(Some(ScalarValue::Integer(1)))),
            ),
            Expr::LessEqual(
                Box::new(Expr::Field(0)),
                Box::new(Expr::Literal(Some(ScalarValue::Integer(9)))),
            ),
        ])
    );

    let negative = parse_create_table(
        "CREATE TABLE app.jobs (state TEXT CHECK (state NOT BETWEEN 'a' AND 'z'))",
    )
    .unwrap();
    assert!(matches!(
        negative.check_constraints.first(),
        Some(CheckExpressionSql::Between { negated: true, .. })
    ));
    assert!(
        bind_check_expression(&negative.check_constraints[0], |name| {
            (name == "state").then_some((0, ScalarType::Text))
        })
        .is_ok()
    );
    assert!(
        parse_add_check_constraint("ALTER TABLE app.jobs ADD CHECK (priority BETWEEN 1)").is_err()
    );
    assert!(
        bind_check_expression(&parsed.expression, |name| {
            (name == "priority").then_some((0, ScalarType::Boolean))
        })
        .is_err()
    );
    let grouped = parse_add_check_constraint(
        "ALTER TABLE app.jobs ADD CHECK ((priority >= 1) BETWEEN FALSE AND TRUE)",
    )
    .unwrap();
    assert!(
        bind_check_expression(&grouped.expression, |name| {
            (name == "priority").then_some((0, ScalarType::Integer))
        })
        .is_ok()
    );
    let bounds = parse_add_check_constraint(
        "ALTER TABLE app.jobs ADD CHECK (priority BETWEEN minimum AND maximum)",
    )
    .unwrap();
    assert_eq!(
        bind_check_expression(&bounds.expression, |name| match name {
            "priority" => Some((0, ScalarType::Integer)),
            "minimum" => Some((1, ScalarType::Integer)),
            "maximum" => Some((2, ScalarType::Integer)),
            _ => None,
        })
        .unwrap(),
        Expr::And(vec![
            Expr::GreaterEqual(Box::new(Expr::Field(0)), Box::new(Expr::Field(1))),
            Expr::LessEqual(Box::new(Expr::Field(0)), Box::new(Expr::Field(2))),
        ])
    );
}

#[test]
fn parses_and_binds_null_safe_distinctness_predicates() {
    use crate::authorization::Expr;

    let distinct = parse_add_check_constraint(
        "ALTER TABLE app.items ADD CHECK (code IS DISTINCT FROM 'retired')",
    )
    .unwrap();
    assert_eq!(
        distinct.expression,
        CheckExpressionSql::DistinctFrom {
            expression: Box::new(CheckExpressionSql::Column("code".into())),
            right: Box::new(CheckExpressionSql::Literal(CheckLiteralSql::String(
                "retired".into(),
            ))),
            negated: false,
        }
    );
    assert_eq!(
        bind_check_expression(&distinct.expression, |name| {
            (name == "code").then_some((0, ScalarType::Text))
        })
        .unwrap(),
        Expr::Or(vec![
            Expr::IsNull(Box::new(Expr::Field(0))),
            Expr::NotEqual(
                Box::new(Expr::Field(0)),
                Box::new(Expr::Literal(Some(ScalarValue::Text("retired".into())))),
            ),
        ])
    );

    let not_distinct = parse_create_table(
        "CREATE TABLE app.items (code UUID CHECK (code IS NOT DISTINCT FROM NULL))",
    )
    .unwrap();
    assert!(matches!(
        not_distinct.check_constraints.first(),
        Some(CheckExpressionSql::DistinctFrom { negated: true, .. })
    ));
    assert_eq!(
        bind_check_expression(&not_distinct.check_constraints[0], |name| {
            (name == "code").then_some((0, ScalarType::Uuid))
        })
        .unwrap(),
        Expr::IsNull(Box::new(Expr::Field(0)))
    );

    let expression = parse_add_check_constraint(
        "ALTER TABLE app.items ADD CHECK ((code IS NULL) IS DISTINCT FROM TRUE)",
    )
    .unwrap();
    assert_eq!(
        bind_check_expression(&expression.expression, |name| {
            (name == "code").then_some((0, ScalarType::Text))
        })
        .unwrap(),
        Expr::Or(vec![
            Expr::IsNull(Box::new(Expr::IsNull(Box::new(Expr::Field(0))))),
            Expr::NotEqual(
                Box::new(Expr::IsNull(Box::new(Expr::Field(0)))),
                Box::new(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            ),
        ])
    );

    let columns =
        parse_add_check_constraint("ALTER TABLE app.items ADD CHECK (code IS DISTINCT FROM other)")
            .unwrap();
    let bound = bind_check_expression(&columns.expression, |name| match name {
        "code" => Some((0, ScalarType::Text)),
        "other" => Some((1, ScalarType::Text)),
        _ => None,
    })
    .unwrap();
    assert!(matches!(bound, Expr::Or(ref branches) if branches.len() == 3));
    assert!(crate::expression::encode_expression(&bound).is_ok());
    assert!(
        bind_check_expression(&columns.expression, |name| match name {
            "code" => Some((0, ScalarType::Text)),
            "other" => Some((1, ScalarType::Integer)),
            _ => None,
        })
        .is_err()
    );

    let literals = parse_add_check_constraint(
        "ALTER TABLE app.items ADD CHECK ('x' IS NOT DISTINCT FROM 'x')",
    )
    .unwrap();
    assert!(bind_check_expression(&literals.expression, |_| None).is_ok());

    for sql in [
        "ALTER TABLE app.items ADD CHECK (code IS DISTINCT)",
        "ALTER TABLE app.items ADD CHECK (code IS DISTINCT FROM)",
    ] {
        assert!(parse_add_check_constraint(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_and_binds_boolean_truth_tests() {
    use crate::authorization::Expr;

    let is_null =
        parse_add_check_constraint("ALTER TABLE app.jobs ADD CHECK ((active = TRUE) IS NULL)")
            .unwrap();
    let equal_true = Expr::Equal(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::Boolean(true)))),
    );
    assert_eq!(
        bind_check_expression(&is_null.expression, |name| {
            (name == "active").then_some((0, ScalarType::Boolean))
        })
        .unwrap(),
        Expr::IsNull(Box::new(equal_true.clone()))
    );
    let is_not_null =
        parse_add_check_constraint("ALTER TABLE app.jobs ADD CHECK ((active = TRUE) IS NOT NULL)")
            .unwrap();
    assert_eq!(
        bind_check_expression(&is_not_null.expression, |name| {
            (name == "active").then_some((0, ScalarType::Boolean))
        })
        .unwrap(),
        Expr::Not(Box::new(Expr::IsNull(Box::new(equal_true))))
    );

    let compound =
        parse_add_check_constraint("ALTER TABLE app.jobs ADD CHECK ((active = TRUE) IS TRUE)")
            .unwrap();
    assert!(matches!(
        &compound.expression,
        CheckExpressionSql::BooleanTest {
            expression,
            test: CheckBooleanTest::True,
            negated: false,
        } if matches!(**expression, CheckExpressionSql::Compare { .. })
    ));
    assert!(
        bind_check_expression(&compound.expression, |name| {
            (name == "active").then_some((0, ScalarType::Boolean))
        })
        .is_ok()
    );
    let role =
        parse_add_check_constraint("ALTER TABLE app.jobs ADD CHECK (HAS_ROLE('operator') IS TRUE)")
            .unwrap();
    assert_eq!(
        bind_policy_expression(
            &role.expression,
            |_| None,
            |name| { (name == "operator").then_some([9; 16]) }
        )
        .unwrap(),
        Expr::And(vec![
            Expr::Not(Box::new(Expr::IsNull(Box::new(Expr::HasRole([9; 16]))))),
            Expr::Equal(
                Box::new(Expr::HasRole([9; 16])),
                Box::new(Expr::Literal(Some(ScalarValue::Boolean(true)))),
            ),
        ])
    );
    assert!(bind_check_expression(&role.expression, |_| None).is_err());

    for (sql, test, negated, expected_value) in [
        ("active IS TRUE", CheckBooleanTest::True, false, Some(true)),
        (
            "active IS NOT TRUE",
            CheckBooleanTest::True,
            true,
            Some(true),
        ),
        (
            "active IS FALSE",
            CheckBooleanTest::False,
            false,
            Some(false),
        ),
        (
            "active IS NOT FALSE",
            CheckBooleanTest::False,
            true,
            Some(false),
        ),
        ("active IS UNKNOWN", CheckBooleanTest::Unknown, false, None),
        (
            "active IS NOT UNKNOWN",
            CheckBooleanTest::Unknown,
            true,
            None,
        ),
    ] {
        let statement =
            parse_add_check_constraint(&format!("ALTER TABLE app.jobs ADD CHECK ({sql})")).unwrap();
        assert_eq!(
            statement.expression,
            CheckExpressionSql::BooleanTest {
                expression: Box::new(CheckExpressionSql::Column("active".into())),
                test,
                negated,
            }
        );
        let expected = match expected_value {
            None => Expr::IsNull(Box::new(Expr::Field(0))),
            Some(value) => Expr::And(vec![
                Expr::Not(Box::new(Expr::IsNull(Box::new(Expr::Field(0))))),
                Expr::Equal(
                    Box::new(Expr::Field(0)),
                    Box::new(Expr::Literal(Some(ScalarValue::Boolean(value)))),
                ),
            ]),
        };
        let expected = if negated {
            Expr::Not(Box::new(expected))
        } else {
            expected
        };
        assert_eq!(
            bind_check_expression(&statement.expression, |name| {
                (name == "active").then_some((0, ScalarType::Boolean))
            })
            .unwrap(),
            expected
        );
    }
    let non_boolean =
        parse_add_check_constraint("ALTER TABLE app.jobs ADD CHECK (state IS TRUE)").unwrap();
    assert!(
        bind_check_expression(&non_boolean.expression, |name| {
            (name == "state").then_some((0, ScalarType::Text))
        })
        .is_err()
    );
    for sql in [
        "ALTER TABLE app.jobs ADD CHECK (active IS MAYBE)",
        "ALTER TABLE app.jobs ADD CHECK (active IS TRUEISH)",
    ] {
        assert!(parse_add_check_constraint(sql).is_err(), "accepted {sql}");
    }
}

#[test]
fn parses_tpcc_and_tpch_column_type_aliases() {
    let parsed = parse_create_table(
        "CREATE TABLE tpcc.customer (
            c_id INTEGER NOT NULL,
            c_middle CHAR(2) NOT NULL,
            c_flag CHAR,
            c_name CHARACTER(16),
            c_since TIMESTAMP,
            c_balance NUMERIC(12, 2),
            PRIMARY KEY (c_id)
        );",
    )
    .unwrap();
    assert_eq!(
        parsed.columns[1].scalar_type,
        ScalarType::Varchar { max_chars: 2 }
    );
    assert_eq!(
        parsed.columns[2].scalar_type,
        ScalarType::Varchar { max_chars: 1 }
    );
    assert_eq!(
        parsed.columns[3].scalar_type,
        ScalarType::Varchar { max_chars: 16 }
    );
    assert_eq!(parsed.columns[4].scalar_type, ScalarType::TimestampTz);
    assert_eq!(
        parsed.columns[5].scalar_type,
        ScalarType::Decimal {
            precision: 12,
            scale: 2
        }
    );
}
