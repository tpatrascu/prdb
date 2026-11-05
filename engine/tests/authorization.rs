use prdb_engine::authorization::{
    AccessTarget, AuthorizationContext, AuthorizationError, Effect, Expr, Grant, Membership,
    Operation, Policy, SanitizedCell, Subject, authorize, check_constraint_passes,
    materialize_default, project_row, resolve_role_closure,
};
use prdb_engine::tuple::ScalarValue;
use std::collections::HashSet;

fn context() -> AuthorizationContext {
    AuthorizationContext {
        principal_id: [1; 16],
        role_ids: HashSet::from([[2; 16]]),
        scope: HashSet::from([(Operation::Select, AccessTarget::Table([3; 16]))]),
    }
}

fn grant() -> Grant {
    Grant {
        subject: Subject::Role([2; 16]),
        operation: Operation::Select,
        target: AccessTarget::Table([3; 16]),
    }
}

fn policy(effect: Effect, using: Expr) -> Policy {
    Policy {
        subjects: vec![Subject::Role([2; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table([3; 16]),
        effect,
        using: Some(using),
        with_check: None,
    }
}

fn boolean(value: bool) -> Expr {
    Expr::Literal(Some(ScalarValue::Boolean(value)))
}

#[test]
fn check_constraints_accept_true_and_unknown_but_reject_false() {
    let ctx = context();
    let row = [Some(ScalarValue::Integer(4)), None];
    let positive = Expr::Greater(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::Integer(0)))),
    );
    let null_test = Expr::Greater(
        Box::new(Expr::Field(1)),
        Box::new(Expr::Literal(Some(ScalarValue::Integer(0)))),
    );
    let negative = Expr::Less(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::Integer(0)))),
    );
    assert_eq!(check_constraint_passes(&positive, &row, &ctx), Ok(true));
    assert_eq!(check_constraint_passes(&null_test, &row, &ctx), Ok(true));
    assert_eq!(check_constraint_passes(&negative, &row, &ctx), Ok(false));
}

#[test]
fn defaults_allow_typed_literals_and_the_admitted_principal_only() {
    let ctx = context();
    assert_eq!(
        materialize_default(
            &Expr::Literal(Some(ScalarValue::Text("default".to_owned()))),
            &ctx,
        ),
        Ok(Some(ScalarValue::Text("default".to_owned())))
    );
    assert_eq!(
        materialize_default(&Expr::CurrentPrincipalId, &ctx),
        Ok(Some(ScalarValue::Uuid(ctx.principal_id)))
    );
    assert_eq!(
        materialize_default(&Expr::Field(0), &ctx),
        Err(AuthorizationError::InvalidPolicy)
    );
}

#[test]
fn authorization_requires_grant_scope_and_an_allow_policy() {
    let ctx = context();
    let row = &[];
    let allow = [policy(Effect::Allow, boolean(true))];
    assert_eq!(
        authorize(
            &ctx,
            &[],
            &allow,
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(row),
            None
        ),
        Err(AuthorizationError::Denied)
    );
    let unscoped = AuthorizationContext {
        scope: HashSet::new(),
        ..ctx.clone()
    };
    assert_eq!(
        authorize(
            &unscoped,
            &[grant()],
            &allow,
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(row),
            None
        ),
        Err(AuthorizationError::Denied)
    );
    assert_eq!(
        authorize(
            &ctx,
            &[grant()],
            &[],
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(row),
            None
        ),
        Err(AuthorizationError::Denied)
    );
    assert_eq!(
        authorize(
            &ctx,
            &[grant()],
            &allow,
            Operation::Select,
            AccessTarget::Table([4; 16]),
            Some(row),
            None
        ),
        Err(AuthorizationError::Denied)
    );
    assert_eq!(
        authorize(
            &ctx,
            &[grant()],
            &allow,
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(row),
            None
        ),
        Ok(())
    );
}

#[test]
fn allow_policies_are_alternatives_and_every_restrict_must_pass() {
    let ctx = context();
    let grants = [grant()];
    let row = &[];
    let policies = [
        policy(Effect::Allow, boolean(false)),
        policy(Effect::Allow, boolean(true)),
        policy(Effect::Restrict, boolean(true)),
        policy(Effect::Restrict, boolean(true)),
    ];
    assert_eq!(
        authorize(
            &ctx,
            &grants,
            &policies,
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(row),
            None
        ),
        Ok(())
    );
    let mut denied = policies.to_vec();
    denied.push(policy(Effect::Restrict, boolean(false)));
    assert_eq!(
        authorize(
            &ctx,
            &grants,
            &denied,
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(row),
            None
        ),
        Err(AuthorizationError::Denied)
    );
}

#[test]
fn null_and_expression_errors_never_authorize() {
    let ctx = context();
    let grants = [grant()];
    let row = &[None];
    let null_policy = [policy(Effect::Allow, Expr::Literal(None))];
    assert_eq!(
        authorize(
            &ctx,
            &grants,
            &null_policy,
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(row),
            None
        ),
        Err(AuthorizationError::Denied)
    );
    let invalid_field = [policy(Effect::Allow, Expr::Field(8))];
    assert_eq!(
        authorize(
            &ctx,
            &grants,
            &invalid_field,
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(row),
            None
        ),
        Err(AuthorizationError::EvaluationFailed)
    );

    let too_many_nodes = Expr::And(vec![boolean(true); 256]);
    assert_eq!(
        authorize(
            &ctx,
            &grants,
            &[policy(Effect::Allow, too_many_nodes)],
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(&[]),
            None,
        ),
        Err(AuthorizationError::EvaluationFailed)
    );
}

#[test]
fn update_allow_must_pass_both_old_and_new_on_the_same_policy() {
    let mut ctx = context();
    ctx.scope
        .insert((Operation::Update, AccessTarget::Table([3; 16])));
    let grant = Grant {
        operation: Operation::Update,
        ..grant()
    };
    let make = |using, with_check| Policy {
        subjects: vec![Subject::Role([2; 16])],
        operation: Operation::Update,
        target: AccessTarget::Table([3; 16]),
        effect: Effect::Allow,
        using: Some(using),
        with_check: Some(with_check),
    };
    let split = [
        make(boolean(true), boolean(false)),
        make(boolean(false), boolean(true)),
    ];
    assert_eq!(
        authorize(
            &ctx,
            &[grant],
            &split,
            Operation::Update,
            AccessTarget::Table([3; 16]),
            Some(&[]),
            Some(&[])
        ),
        Err(AuthorizationError::Denied)
    );
    let paired = [make(boolean(true), boolean(true))];
    assert_eq!(
        authorize(
            &ctx,
            &[grant],
            &paired,
            Operation::Update,
            AccessTarget::Table([3; 16]),
            Some(&[]),
            Some(&[])
        ),
        Ok(())
    );
}

#[test]
fn role_and_principal_recipients_use_only_the_pinned_context() {
    let mut ctx = context();
    ctx.scope
        .insert((Operation::Select, AccessTarget::Table([4; 16])));
    let direct_grant = Grant {
        subject: Subject::Principal([1; 16]),
        operation: Operation::Select,
        target: AccessTarget::Table([4; 16]),
    };
    let direct_policy = Policy {
        subjects: vec![Subject::Principal([1; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table([4; 16]),
        effect: Effect::Allow,
        using: Some(Expr::HasRole([8; 16])),
        with_check: None,
    };
    assert_eq!(
        authorize(
            &ctx,
            &[direct_grant],
            &[direct_policy],
            Operation::Select,
            AccessTarget::Table([4; 16]),
            Some(&[]),
            None
        ),
        Err(AuthorizationError::Denied)
    );
}

#[test]
fn typed_predicates_use_three_valued_logic_and_enforce_work_limits() {
    let ctx = context();
    let grants = [grant()];
    let row = [Some(ScalarValue::SmallInt(9))];
    let greater = Expr::Greater(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::SmallInt(7)))),
    );
    assert_eq!(
        authorize(
            &ctx,
            &grants,
            &[policy(Effect::Allow, greater)],
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(&row),
            None,
        ),
        Ok(())
    );

    let row_with_null = [None];
    let unknown_or_true = Expr::Or(vec![
        Expr::Field(0),
        Expr::Literal(Some(ScalarValue::Boolean(true))),
    ]);
    assert_eq!(
        authorize(
            &ctx,
            &grants,
            &[policy(Effect::Allow, unknown_or_true)],
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(&row_with_null),
            None,
        ),
        Ok(())
    );

    let mismatched = Expr::Equal(
        Box::new(Expr::Field(0)),
        Box::new(Expr::Literal(Some(ScalarValue::BigInt(9)))),
    );
    assert_eq!(
        authorize(
            &ctx,
            &grants,
            &[policy(Effect::Allow, mismatched)],
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(&row),
            None,
        ),
        Err(AuthorizationError::EvaluationFailed)
    );

    let too_deep = (0..33).fold(boolean(true), |expression, _| {
        Expr::Not(Box::new(expression))
    });
    assert_eq!(
        authorize(
            &ctx,
            &grants,
            &[policy(Effect::Allow, too_deep)],
            Operation::Select,
            AccessTarget::Table([3; 16]),
            Some(&[]),
            None,
        ),
        Err(AuthorizationError::EvaluationFailed)
    );
}

#[test]
fn row_projection_redacts_cells_without_disclosing_hidden_payload_lengths() {
    let table_id = [3; 16];
    let name_id = [10; 16];
    let manager_id = [11; 16];
    let secret_id = [12; 16];
    let optional_id = [13; 16];
    let columns = [name_id, manager_id, secret_id, optional_id];
    let row = [
        Some(ScalarValue::Text("Alice".to_owned())),
        Some(ScalarValue::Uuid([1; 16])),
        Some(ScalarValue::Text(
            "a secret with a distinctive length".to_owned(),
        )),
        None,
    ];
    let make_context = |principal_id| AuthorizationContext {
        principal_id,
        role_ids: HashSet::from([[2; 16]]),
        scope: HashSet::from([
            (Operation::Usage, AccessTarget::Schema([30; 16])),
            (Operation::Select, AccessTarget::Table(table_id)),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: name_id,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: manager_id,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: secret_id,
                },
            ),
            (
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id: optional_id,
                },
            ),
        ]),
    };
    let mut grants = [name_id, manager_id, secret_id, optional_id]
        .map(|column_id| Grant {
            subject: Subject::Role([2; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
        })
        .to_vec();
    grants.push(Grant {
        subject: Subject::Role([2; 16]),
        operation: Operation::Usage,
        target: AccessTarget::Schema([30; 16]),
    });
    let policies = [
        Policy {
            subjects: vec![Subject::Role([2; 16])],
            operation: Operation::Select,
            target: AccessTarget::Table(table_id),
            effect: Effect::Allow,
            using: Some(boolean(true)),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([2; 16])],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: name_id,
            },
            effect: Effect::Allow,
            using: Some(boolean(true)),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([2; 16])],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: secret_id,
            },
            effect: Effect::Allow,
            using: Some(Expr::Equal(
                Box::new(Expr::Field(1)),
                Box::new(Expr::CurrentPrincipalId),
            )),
            with_check: None,
        },
        Policy {
            subjects: vec![Subject::Role([2; 16])],
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id: optional_id,
            },
            effect: Effect::Allow,
            using: Some(boolean(true)),
            with_check: None,
        },
    ];

    let manager = project_row(
        &make_context([1; 16]),
        &grants,
        &policies,
        [30; 16],
        table_id,
        &columns,
        &row,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        manager[0],
        SanitizedCell::Value(ScalarValue::Text("Alice".into()))
    );
    assert_eq!(manager[1], SanitizedCell::Redacted);
    assert_eq!(
        manager[2],
        SanitizedCell::Value(ScalarValue::Text(
            "a secret with a distinctive length".to_owned()
        ))
    );
    assert_eq!(manager[3], SanitizedCell::Null);

    let other = project_row(
        &make_context([8; 16]),
        &grants,
        &policies,
        [30; 16],
        table_id,
        &columns,
        &row,
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        other[0],
        SanitizedCell::Value(ScalarValue::Text("Alice".into()))
    );
    assert_eq!(other[2], SanitizedCell::Redacted);
    assert_eq!(
        std::mem::size_of_val(&other[2]),
        std::mem::size_of::<SanitizedCell>()
    );
}

#[test]
fn row_projection_hides_rows_denied_by_the_table_policy() {
    let table_id = [3; 16];
    let column_id = [10; 16];
    let mut ctx = context();
    ctx.scope
        .insert((Operation::Usage, AccessTarget::Schema([30; 16])));
    ctx.scope.insert((
        Operation::Select,
        AccessTarget::Column {
            table_id,
            column_id,
        },
    ));
    let grants = [
        Grant {
            subject: Subject::Role([2; 16]),
            operation: Operation::Select,
            target: AccessTarget::Column {
                table_id,
                column_id,
            },
        },
        Grant {
            subject: Subject::Role([2; 16]),
            operation: Operation::Usage,
            target: AccessTarget::Schema([30; 16]),
        },
    ];
    let policies = [Policy {
        subjects: vec![Subject::Role([2; 16])],
        operation: Operation::Select,
        target: AccessTarget::Table(table_id),
        effect: Effect::Allow,
        using: Some(boolean(false)),
        with_check: None,
    }];
    assert_eq!(
        project_row(
            &ctx,
            &grants,
            &policies,
            [30; 16],
            table_id,
            &[column_id],
            &[Some(ScalarValue::Text("hidden row".into()))]
        ),
        Ok(None)
    );

    let mut no_schema_usage = ctx.clone();
    no_schema_usage
        .scope
        .remove(&(Operation::Usage, AccessTarget::Schema([30; 16])));
    assert_eq!(
        project_row(
            &no_schema_usage,
            &grants,
            &policies,
            [30; 16],
            table_id,
            &[column_id],
            &[Some(ScalarValue::Text("hidden row".into()))]
        ),
        Ok(None)
    );
}

#[test]
fn role_closure_is_transitive_bounded_and_cycle_checked() {
    let closure = resolve_role_closure(
        [1; 16],
        &[
            Membership {
                member_id: [1; 16],
                role_id: [2; 16],
            },
            Membership {
                member_id: [1; 16],
                role_id: [3; 16],
            },
            Membership {
                member_id: [2; 16],
                role_id: [4; 16],
            },
            Membership {
                member_id: [3; 16],
                role_id: [4; 16],
            },
        ],
    )
    .unwrap();
    assert_eq!(closure, HashSet::from([[2; 16], [3; 16], [4; 16]]));

    assert_eq!(
        resolve_role_closure(
            [1; 16],
            &[
                Membership {
                    member_id: [1; 16],
                    role_id: [2; 16],
                },
                Membership {
                    member_id: [2; 16],
                    role_id: [1; 16],
                },
            ],
        ),
        Err(AuthorizationError::InvalidPolicy)
    );
    assert_eq!(
        resolve_role_closure(
            [1; 16],
            &[
                Membership {
                    member_id: [1; 16],
                    role_id: [2; 16],
                },
                Membership {
                    member_id: [1; 16],
                    role_id: [2; 16],
                },
            ],
        ),
        Err(AuthorizationError::InvalidPolicy)
    );
    let too_many = vec![
        Membership {
            member_id: [1; 16],
            role_id: [2; 16],
        };
        prdb_engine::authorization::MAX_MEMBERSHIP_EDGES + 1
    ];
    assert_eq!(
        resolve_role_closure([1; 16], &too_many),
        Err(AuthorizationError::InvalidPolicy)
    );
}

#[test]
fn role_closure_rejects_cycles_even_in_disconnected_membership_components() {
    let edges = [
        Membership {
            member_id: [3; 16],
            role_id: [4; 16],
        },
        Membership {
            member_id: [4; 16],
            role_id: [3; 16],
        },
    ];
    assert_eq!(
        resolve_role_closure([1; 16], &edges),
        Err(AuthorizationError::InvalidPolicy)
    );
}
