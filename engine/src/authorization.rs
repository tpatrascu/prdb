//! Bounded, fail-closed evaluation of pinned grants and row/cell policies.
//!
//! Policy expressions are trusted storage-side IR. They contain no callbacks,
//! I/O, or worker-provided variables. The caller supplies rows decoded from its
//! pinned snapshot/private overlay and an identity/role closure from the same
//! pinned authority state.

use crate::tuple::{ColumnSchema, ScalarValue};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

pub const MAX_EXPRESSION_NODES: usize = 256;
pub const MAX_EXPRESSION_DEPTH: usize = 32;
pub const MAX_AUTHORIZATION_INPUTS: usize = 4096;
pub const MAX_POLICY_SUBJECTS: usize = 256;
pub const MAX_MEMBERSHIP_EDGES: usize = 4096;

type Id = [u8; 16];

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Operation {
    Usage,
    Select,
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum AccessTarget {
    Schema(Id),
    Table(Id),
    Column { table_id: Id, column_id: Id },
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Subject {
    Principal(Id),
    Role(Id),
}

/// Pinned directed membership edge from a user/role to an inherited role.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Membership {
    pub member_id: Id,
    pub role_id: Id,
}

/// Reject malformed membership graphs, including cycles outside any one
/// principal's reachable role closure. Administrative catalog commits use this
/// before publishing membership records.
pub fn validate_membership_graph(memberships: &[Membership]) -> Result<(), AuthorizationError> {
    if memberships.len() > MAX_MEMBERSHIP_EDGES {
        return Err(AuthorizationError::InvalidPolicy);
    }
    let mut adjacency: HashMap<Id, Vec<Id>> = HashMap::new();
    let mut unique_edges = HashSet::with_capacity(memberships.len());
    for edge in memberships {
        if edge.member_id == [0; 16]
            || edge.role_id == [0; 16]
            || !unique_edges.insert((edge.member_id, edge.role_id))
        {
            return Err(AuthorizationError::InvalidPolicy);
        }
        adjacency
            .entry(edge.member_id)
            .or_default()
            .push(edge.role_id);
    }

    let mut state = HashMap::<Id, u8>::new();
    for start in adjacency.keys().copied() {
        if state.get(&start) == Some(&2) {
            continue;
        }
        state.insert(start, 1);
        let mut stack = vec![(start, 0usize)];
        while let Some((member_id, next_edge)) = stack.last_mut() {
            let next_role = adjacency
                .get(member_id)
                .and_then(|roles| roles.get(*next_edge))
                .copied();
            let Some(role_id) = next_role else {
                state.insert(*member_id, 2);
                stack.pop();
                continue;
            };
            *next_edge += 1;
            match state.get(&role_id).copied() {
                Some(1) => return Err(AuthorizationError::InvalidPolicy),
                Some(2) => {}
                _ => {
                    state.insert(role_id, 1);
                    stack.push((role_id, 0));
                }
            }
        }
    }
    Ok(())
}

/// Resolve the transitive role closure for one principal and reject malformed,
/// duplicate, cyclic, or over-budget membership graphs.
pub fn resolve_role_closure(
    principal_id: Id,
    memberships: &[Membership],
) -> Result<HashSet<Id>, AuthorizationError> {
    if principal_id == [0; 16] {
        return Err(AuthorizationError::InvalidPolicy);
    }
    validate_membership_graph(memberships)?;
    let mut adjacency: HashMap<Id, Vec<Id>> = HashMap::new();
    let mut unique_edges = HashSet::with_capacity(memberships.len());
    for edge in memberships {
        if edge.member_id == [0; 16]
            || edge.role_id == [0; 16]
            || !unique_edges.insert((edge.member_id, edge.role_id))
        {
            return Err(AuthorizationError::InvalidPolicy);
        }
        adjacency
            .entry(edge.member_id)
            .or_default()
            .push(edge.role_id);
    }

    let mut state = HashMap::<Id, u8>::new();
    let mut closure = HashSet::new();
    let mut stack = vec![(principal_id, 0usize)];
    let mut visited_edges = 0;
    state.insert(principal_id, 1);
    while let Some((member_id, next_edge)) = stack.last_mut() {
        let next_role = adjacency
            .get(member_id)
            .and_then(|roles| roles.get(*next_edge))
            .copied();
        let Some(role_id) = next_role else {
            state.insert(*member_id, 2);
            stack.pop();
            continue;
        };
        *next_edge += 1;
        visited_edges += 1;
        if visited_edges > MAX_MEMBERSHIP_EDGES {
            return Err(AuthorizationError::InvalidPolicy);
        }
        closure.insert(role_id);
        match state.get(&role_id).copied() {
            Some(1) => return Err(AuthorizationError::InvalidPolicy),
            Some(2) => {}
            _ => {
                state.insert(role_id, 1);
                stack.push((role_id, 0));
            }
        }
    }
    Ok(closure)
}

#[derive(Clone, Debug)]
pub struct AuthorizationContext {
    pub principal_id: Id,
    /// Transitive, cycle-checked role closure from the pinned catalog snapshot.
    pub role_ids: HashSet<Id>,
    /// The caller-supplied invocation capability, already narrowed by admission.
    pub scope: HashSet<(Operation, AccessTarget)>,
}

/// Exact catalog state from the pinned snapshot that produced an authority or
/// schema input used by an operation. `None` guards a negative lookup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorityDependency {
    pub key: Vec<u8>,
    pub expected_value: Option<Vec<u8>>,
}

/// Immutable pinned schema and authority inputs for one authorized row read.
pub struct AuthorizedProjection<'a> {
    pub columns: &'a [ColumnSchema],
    pub column_ids: &'a [[u8; 16]],
    pub schema_version: u64,
    pub schema_id: [u8; 16],
    pub context: &'a AuthorizationContext,
    pub grants: &'a [Grant],
    pub policies: &'a [Policy],
    /// Trusted, schema-bound CHECK expressions applied to proposed rows.
    pub checks: &'a [Expr],
    /// Columns identified by the pinned policy compiler as influencing access.
    pub policy_driving_columns: &'a [[u8; 16]],
    /// All principal, role, grant, policy, and schema records used to build this context.
    pub dependencies: &'a [AuthorityDependency],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Grant {
    pub subject: Subject,
    pub operation: Operation,
    pub target: AccessTarget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Effect {
    Allow,
    Restrict,
}

/// Small expression tree for the initial trusted policy language. Identifiers
/// in `HasRole` are bound to catalog role IDs when the policy is created.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Expr {
    Literal(Option<ScalarValue>),
    Field(usize),
    CurrentPrincipalId,
    HasRole(Id),
    Equal(Box<Expr>, Box<Expr>),
    NotEqual(Box<Expr>, Box<Expr>),
    Less(Box<Expr>, Box<Expr>),
    LessEqual(Box<Expr>, Box<Expr>),
    Greater(Box<Expr>, Box<Expr>),
    GreaterEqual(Box<Expr>, Box<Expr>),
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
    IsNull(Box<Expr>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Policy {
    pub subjects: Vec<Subject>,
    pub operation: Operation,
    pub target: AccessTarget,
    pub effect: Effect,
    /// Old/visible-row predicate; required for SELECT, DELETE, and UPDATE.
    pub using: Option<Expr>,
    /// Proposed-row predicate; required for INSERT and UPDATE.
    pub with_check: Option<Expr>,
}

/// Sanitized cell output. `Redacted` deliberately carries no value or length.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SanitizedCell {
    Value(ScalarValue),
    Null,
    Redacted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthorizationError {
    Denied,
    EvaluationFailed,
    InvalidPolicy,
}

impl std::fmt::Display for AuthorizationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "authorization failed: {self:?}")
    }
}

impl std::error::Error for AuthorizationError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Truth {
    True,
    False,
    Unknown,
}

enum Value {
    Scalar(Option<ScalarValue>),
}

/// Check the invocation capability and direct/inherited grant, then apply
/// matching row/cell policies. Ownership and administrative privileges never
/// bypass either check. For UPDATE, each ALLOW policy must accept both rows.
pub fn authorize(
    context: &AuthorizationContext,
    grants: &[Grant],
    policies: &[Policy],
    operation: Operation,
    target: AccessTarget,
    old_row: Option<&[Option<ScalarValue>]>,
    new_row: Option<&[Option<ScalarValue>]>,
) -> Result<(), AuthorizationError> {
    validate_rows(operation, old_row, new_row)?;
    validate_input_sizes(context, grants.len(), policies)?;
    if !has_permission(context, grants, operation, target) {
        return Err(AuthorizationError::Denied);
    }

    evaluate_policies(context, policies, operation, target, old_row, new_row)
}

/// Evaluate one trusted CHECK constraint against a complete proposed row.
/// SQL CHECK accepts TRUE and UNKNOWN (NULL), and rejects only FALSE.
pub fn check_constraint_passes(
    expression: &Expr,
    row: &[Option<ScalarValue>],
    context: &AuthorizationContext,
) -> Result<bool, AuthorizationError> {
    if row.len() > crate::tuple::MAX_COLUMNS {
        return Err(AuthorizationError::InvalidPolicy);
    }
    let mut remaining = MAX_EXPRESSION_NODES;
    let value = eval(expression, row, context, &mut remaining, 1)?;
    match as_truth(value)? {
        Truth::False => Ok(false),
        Truth::True | Truth::Unknown => Ok(true),
    }
}

/// Materialize one supported INSERT default. SQL defaults are restricted to
/// typed literals and the trusted current-principal value in this increment.
pub fn materialize_default(
    expression: &Expr,
    context: &AuthorizationContext,
) -> Result<Option<ScalarValue>, AuthorizationError> {
    match expression {
        Expr::Literal(value) => Ok(value.clone()),
        Expr::CurrentPrincipalId => Ok(Some(ScalarValue::Uuid(context.principal_id))),
        _ => Err(AuthorizationError::InvalidPolicy),
    }
}

/// Return one authorized row projection. The row-level SELECT policy is
/// checked before cells; rows with no readable granted field are invisible.
/// Individually denied fields become payload-free REDACTED cells.
pub fn project_row(
    context: &AuthorizationContext,
    grants: &[Grant],
    policies: &[Policy],
    schema_id: Id,
    table_id: Id,
    column_ids: &[Id],
    row: &[Option<ScalarValue>],
) -> Result<Option<Vec<SanitizedCell>>, AuthorizationError> {
    if !row_visible(
        context, grants, policies, schema_id, table_id, column_ids, row,
    )? {
        return Ok(None);
    }
    let mut projected = Vec::with_capacity(row.len());
    for (&column_id, value) in column_ids.iter().zip(row) {
        let target = AccessTarget::Column {
            table_id,
            column_id,
        };
        if !has_permission(context, grants, Operation::Select, target) {
            projected.push(SanitizedCell::Redacted);
            continue;
        }
        match evaluate_policies(
            context,
            policies,
            Operation::Select,
            target,
            Some(row),
            None,
        ) {
            Ok(()) => projected.push(match value {
                Some(value) => SanitizedCell::Value(value.clone()),
                None => SanitizedCell::Null,
            }),
            Err(AuthorizationError::Denied) => projected.push(SanitizedCell::Redacted),
            Err(error) => return Err(error),
        }
    }
    Ok(Some(projected))
}

/// Determine row visibility without evaluating or returning individual cells.
pub fn row_visible(
    context: &AuthorizationContext,
    grants: &[Grant],
    policies: &[Policy],
    schema_id: Id,
    table_id: Id,
    column_ids: &[Id],
    row: &[Option<ScalarValue>],
) -> Result<bool, AuthorizationError> {
    validate_input_sizes(context, grants.len(), policies)?;
    if row.len() != column_ids.len()
        || row.len() > crate::tuple::MAX_COLUMNS
        || column_ids.contains(&[0; 16])
        || column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len()
    {
        return Err(AuthorizationError::InvalidPolicy);
    }
    let table_target = AccessTarget::Table(table_id);
    if !has_permission(
        context,
        grants,
        Operation::Usage,
        AccessTarget::Schema(schema_id),
    ) || !context.scope.contains(&(Operation::Select, table_target))
        || !column_ids.iter().any(|&column_id| {
            has_permission(
                context,
                grants,
                Operation::Select,
                AccessTarget::Column {
                    table_id,
                    column_id,
                },
            )
        })
    {
        return Ok(false);
    }
    match evaluate_policies(
        context,
        policies,
        Operation::Select,
        table_target,
        Some(row),
        None,
    ) {
        Ok(()) => Ok(true),
        Err(AuthorizationError::Denied) => Ok(false),
        Err(error) => Err(error),
    }
}

fn validate_input_sizes(
    context: &AuthorizationContext,
    grant_count: usize,
    policies: &[Policy],
) -> Result<(), AuthorizationError> {
    if grant_count > MAX_AUTHORIZATION_INPUTS
        || policies.len() > MAX_AUTHORIZATION_INPUTS
        || context.role_ids.len() > MAX_AUTHORIZATION_INPUTS
        || context.scope.len() > MAX_AUTHORIZATION_INPUTS
        || policies
            .iter()
            .any(|policy| policy.subjects.len() > MAX_POLICY_SUBJECTS)
    {
        Err(AuthorizationError::InvalidPolicy)
    } else {
        Ok(())
    }
}

/// Whether a capability exists in both the admitted invocation scope and the
/// principal's direct/inherited grant set.
pub fn has_permission(
    context: &AuthorizationContext,
    grants: &[Grant],
    operation: Operation,
    target: AccessTarget,
) -> bool {
    context.scope.contains(&(operation, target))
        && grants.iter().any(|grant| {
            grant.operation == operation
                && grant.target == target
                && matches_subject(context, grant.subject)
        })
}

fn evaluate_policies(
    context: &AuthorizationContext,
    policies: &[Policy],
    operation: Operation,
    target: AccessTarget,
    old_row: Option<&[Option<ScalarValue>]>,
    new_row: Option<&[Option<ScalarValue>]>,
) -> Result<(), AuthorizationError> {
    let mut has_applicable_allow = false;
    let mut allowed = false;
    for policy in policies.iter().filter(|policy| {
        policy.operation == operation
            && policy.target == target
            && policy
                .subjects
                .iter()
                .any(|&subject| matches_subject(context, subject))
    }) {
        validate_policy_shape(policy)?;
        let passes = match operation {
            Operation::Usage => return Err(AuthorizationError::InvalidPolicy),
            Operation::Select | Operation::Delete => {
                predicate_passes(policy.using.as_ref().unwrap(), old_row.unwrap(), context)?
            }
            Operation::Insert => predicate_passes(
                policy.with_check.as_ref().unwrap(),
                new_row.unwrap(),
                context,
            )?,
            Operation::Update => {
                let using =
                    predicate_passes(policy.using.as_ref().unwrap(), old_row.unwrap(), context)?;
                let check = predicate_passes(
                    policy.with_check.as_ref().unwrap(),
                    new_row.unwrap(),
                    context,
                )?;
                using && check
            }
        };
        match policy.effect {
            Effect::Allow => {
                has_applicable_allow = true;
                allowed |= passes;
            }
            Effect::Restrict if !passes => return Err(AuthorizationError::Denied),
            Effect::Restrict => {}
        }
    }
    if has_applicable_allow && allowed {
        Ok(())
    } else {
        Err(AuthorizationError::Denied)
    }
}

fn validate_rows(
    operation: Operation,
    old_row: Option<&[Option<ScalarValue>]>,
    new_row: Option<&[Option<ScalarValue>]>,
) -> Result<(), AuthorizationError> {
    let shape_ok = match operation {
        Operation::Usage => false,
        Operation::Select | Operation::Delete => old_row.is_some() && new_row.is_none(),
        Operation::Insert => old_row.is_none() && new_row.is_some(),
        Operation::Update => old_row.is_some() && new_row.is_some(),
    };
    if !shape_ok
        || old_row.is_some_and(|row| row.len() > crate::tuple::MAX_COLUMNS)
        || new_row.is_some_and(|row| row.len() > crate::tuple::MAX_COLUMNS)
    {
        Err(AuthorizationError::InvalidPolicy)
    } else {
        Ok(())
    }
}

fn validate_policy_shape(policy: &Policy) -> Result<(), AuthorizationError> {
    let valid = !policy.subjects.is_empty()
        && policy.subjects.len() <= MAX_POLICY_SUBJECTS
        && match policy.operation {
            Operation::Usage => false,
            Operation::Select | Operation::Delete => {
                policy.using.is_some() && policy.with_check.is_none()
            }
            Operation::Insert => policy.using.is_none() && policy.with_check.is_some(),
            Operation::Update => policy.using.is_some() && policy.with_check.is_some(),
        };
    if valid {
        Ok(())
    } else {
        Err(AuthorizationError::InvalidPolicy)
    }
}

fn matches_subject(context: &AuthorizationContext, subject: Subject) -> bool {
    match subject {
        Subject::Principal(id) => context.principal_id == id,
        Subject::Role(id) => context.role_ids.contains(&id),
    }
}

fn predicate_passes(
    expression: &Expr,
    row: &[Option<ScalarValue>],
    context: &AuthorizationContext,
) -> Result<bool, AuthorizationError> {
    let mut remaining = MAX_EXPRESSION_NODES;
    let value = eval(expression, row, context, &mut remaining, 1)?;
    match value {
        Value::Scalar(Some(ScalarValue::Boolean(true))) => Ok(true),
        Value::Scalar(Some(ScalarValue::Boolean(false))) | Value::Scalar(None) => Ok(false),
        _ => Err(AuthorizationError::EvaluationFailed),
    }
}

fn eval(
    expression: &Expr,
    row: &[Option<ScalarValue>],
    context: &AuthorizationContext,
    remaining: &mut usize,
    depth: usize,
) -> Result<Value, AuthorizationError> {
    if depth > MAX_EXPRESSION_DEPTH || *remaining == 0 {
        return Err(AuthorizationError::EvaluationFailed);
    }
    *remaining -= 1;
    let child = |expr: &Expr, remaining: &mut usize| eval(expr, row, context, remaining, depth + 1);
    match expression {
        Expr::Literal(value) => Ok(Value::Scalar(value.clone())),
        Expr::Field(index) => row
            .get(*index)
            .cloned()
            .map(Value::Scalar)
            .ok_or(AuthorizationError::EvaluationFailed),
        Expr::CurrentPrincipalId => {
            Ok(Value::Scalar(Some(ScalarValue::Uuid(context.principal_id))))
        }
        Expr::HasRole(role_id) => Ok(Value::Scalar(Some(ScalarValue::Boolean(
            context.role_ids.contains(role_id),
        )))),
        Expr::Equal(left, right)
        | Expr::NotEqual(left, right)
        | Expr::Less(left, right)
        | Expr::LessEqual(left, right)
        | Expr::Greater(left, right)
        | Expr::GreaterEqual(left, right) => {
            let Value::Scalar(left) = child(left, remaining)?;
            let Value::Scalar(right) = child(right, remaining)?;
            let Some((left, right)) = left.zip(right) else {
                return Ok(Value::Scalar(None));
            };
            let ordering = compare_scalars(&left, &right)?;
            let result = match expression {
                Expr::Equal(_, _) => ordering == Ordering::Equal,
                Expr::NotEqual(_, _) => ordering != Ordering::Equal,
                Expr::Less(_, _) => ordering == Ordering::Less,
                Expr::LessEqual(_, _) => ordering != Ordering::Greater,
                Expr::Greater(_, _) => ordering == Ordering::Greater,
                Expr::GreaterEqual(_, _) => ordering != Ordering::Less,
                _ => unreachable!(),
            };
            Ok(Value::Scalar(Some(ScalarValue::Boolean(result))))
        }
        Expr::And(expressions) => {
            let mut result = Truth::True;
            for expression in expressions {
                match as_truth(child(expression, remaining)?)? {
                    Truth::False => return Ok(Value::Scalar(Some(ScalarValue::Boolean(false)))),
                    Truth::Unknown => result = Truth::Unknown,
                    Truth::True => {}
                }
            }
            Ok(truth_value(result))
        }
        Expr::Or(expressions) => {
            let mut result = Truth::False;
            for expression in expressions {
                match as_truth(child(expression, remaining)?)? {
                    Truth::True => return Ok(Value::Scalar(Some(ScalarValue::Boolean(true)))),
                    Truth::Unknown => result = Truth::Unknown,
                    Truth::False => {}
                }
            }
            Ok(truth_value(result))
        }
        Expr::Not(expression) => {
            let result = match as_truth(child(expression, remaining)?)? {
                Truth::True => Truth::False,
                Truth::False => Truth::True,
                Truth::Unknown => Truth::Unknown,
            };
            Ok(truth_value(result))
        }
        Expr::IsNull(expression) => {
            let Value::Scalar(value) = child(expression, remaining)?;
            Ok(Value::Scalar(Some(ScalarValue::Boolean(value.is_none()))))
        }
    }
}

fn as_truth(value: Value) -> Result<Truth, AuthorizationError> {
    match value {
        Value::Scalar(Some(ScalarValue::Boolean(true))) => Ok(Truth::True),
        Value::Scalar(Some(ScalarValue::Boolean(false))) => Ok(Truth::False),
        Value::Scalar(None) => Ok(Truth::Unknown),
        _ => Err(AuthorizationError::EvaluationFailed),
    }
}

fn truth_value(value: Truth) -> Value {
    match value {
        Truth::True => Value::Scalar(Some(ScalarValue::Boolean(true))),
        Truth::False => Value::Scalar(Some(ScalarValue::Boolean(false))),
        Truth::Unknown => Value::Scalar(None),
    }
}

fn compare_scalars(
    left: &ScalarValue,
    right: &ScalarValue,
) -> Result<Ordering, AuthorizationError> {
    let ordering = match (left, right) {
        (ScalarValue::Boolean(left), ScalarValue::Boolean(right)) => left.cmp(right),
        (ScalarValue::SmallInt(left), ScalarValue::SmallInt(right)) => left.cmp(right),
        (ScalarValue::Integer(left), ScalarValue::Integer(right)) => left.cmp(right),
        (ScalarValue::BigInt(left), ScalarValue::BigInt(right)) => left.cmp(right),
        (ScalarValue::Decimal(left), ScalarValue::Decimal(right)) => left.cmp(right),
        (ScalarValue::Text(left), ScalarValue::Text(right)) => left.cmp(right),
        (ScalarValue::Varchar(left), ScalarValue::Varchar(right)) => left.cmp(right),
        (ScalarValue::Bytea(left), ScalarValue::Bytea(right)) => left.cmp(right),
        (ScalarValue::Uuid(left), ScalarValue::Uuid(right)) => left.cmp(right),
        (ScalarValue::Date(left), ScalarValue::Date(right)) => left.cmp(right),
        (ScalarValue::TimestampTz(left), ScalarValue::TimestampTz(right)) => left.cmp(right),
        _ => return Err(AuthorizationError::EvaluationFailed),
    };
    Ok(ordering)
}
