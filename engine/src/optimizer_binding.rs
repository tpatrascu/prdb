//! Bind structured optimizer requests against invocation-authorized metadata.

use crate::metadata_discovery::{AuthorizedRelationDescription, VisibleColumn};
use crate::optimizer_request::{Comparison, Predicate, RequestError, decode_select_request};
use crate::tuple::ScalarType;
use std::collections::HashSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundSelect {
    pub relation_id: [u8; 16],
    pub projections: Vec<BoundColumn>,
    pub predicate: Option<BoundPredicate>,
    pub limit: Option<u32>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BoundColumn {
    pub column_id: [u8; 16],
    pub ordinal: u32,
    pub scalar_type: ScalarType,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BoundPredicate {
    Null,
    Boolean(bool),
    Integer(i64),
    Text(String),
    Column(BoundColumn),
    Compare {
        operator: Comparison,
        left: Box<BoundPredicate>,
        right: Box<BoundPredicate>,
    },
    And(Vec<BoundPredicate>),
    Or(Vec<BoundPredicate>),
    Not(Box<BoundPredicate>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BindError {
    Request(RequestError),
    RelationUnavailable,
    InvalidMetadata,
    InvalidQuery,
}

impl std::fmt::Display for BindError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "optimizer bind error: {self:?}")
    }
}

impl std::error::Error for BindError {}

impl From<RequestError> for BindError {
    fn from(error: RequestError) -> Self {
        Self::Request(error)
    }
}

/// Bind one canonical request against a relation description returned by the
/// invocation's authorized planner metadata connection. Missing and
/// undiscoverable relations share one result and never produce a partial plan.
pub fn bind_select_request(
    request_bytes: &[u8],
    relation: Option<AuthorizedRelationDescription>,
) -> Result<BoundSelect, BindError> {
    let request = decode_select_request(request_bytes)?;
    let relation = relation.ok_or(BindError::RelationUnavailable)?;
    if !relation.name.eq_ignore_ascii_case(&request.relation_name) {
        return Err(BindError::RelationUnavailable);
    }
    validate_relation_shape(&relation)?;
    let mut projections = Vec::with_capacity(request.projections.len());
    for name in &request.projections {
        projections.push(resolve_column(&relation.columns, name)?.into());
    }
    let predicate = request
        .predicate
        .as_ref()
        .map(|predicate| {
            let (bound, scalar_type) = bind_predicate(predicate, &relation.columns)?;
            if scalar_type != Some(ScalarType::Boolean) {
                return Err(BindError::InvalidQuery);
            }
            Ok(bound)
        })
        .transpose()?;
    Ok(BoundSelect {
        relation_id: relation.relation_id,
        projections,
        predicate,
        limit: request.limit,
    })
}

fn validate_relation_shape(relation: &AuthorizedRelationDescription) -> Result<(), BindError> {
    if relation.relation_id == [0; 16] || relation.columns.is_empty() {
        return Err(BindError::InvalidMetadata);
    }
    let mut ids = HashSet::with_capacity(relation.columns.len());
    let mut ordinals = HashSet::with_capacity(relation.columns.len());
    for column in &relation.columns {
        if column.column_id == [0; 16]
            || column.name.is_empty()
            || column.name.len() > 63
            || !column.name.is_ascii()
            || !ids.insert(column.column_id)
            || !ordinals.insert(column.ordinal)
        {
            return Err(BindError::InvalidMetadata);
        }
    }
    if ordinals.len() != relation.columns.len()
        || (0..relation.columns.len()).any(|ordinal| !ordinals.contains(&(ordinal as u32)))
    {
        return Err(BindError::InvalidMetadata);
    }
    Ok(())
}

fn resolve_column<'a>(
    columns: &'a [VisibleColumn],
    name: &str,
) -> Result<&'a VisibleColumn, BindError> {
    let mut matching = columns
        .iter()
        .filter(|column| column.name.eq_ignore_ascii_case(name));
    let column = matching.next().ok_or(BindError::InvalidQuery)?;
    if matching.next().is_some() {
        return Err(BindError::InvalidMetadata);
    }
    Ok(column)
}

impl From<&VisibleColumn> for BoundColumn {
    fn from(column: &VisibleColumn) -> Self {
        Self {
            column_id: column.column_id,
            ordinal: column.ordinal,
            scalar_type: column.scalar_type,
            nullable: column.nullable,
        }
    }
}

fn bind_predicate(
    predicate: &Predicate,
    columns: &[VisibleColumn],
) -> Result<(BoundPredicate, Option<ScalarType>), BindError> {
    match predicate {
        Predicate::Null => Ok((BoundPredicate::Null, None)),
        Predicate::Boolean(value) => {
            Ok((BoundPredicate::Boolean(*value), Some(ScalarType::Boolean)))
        }
        Predicate::Integer(value) => {
            Ok((BoundPredicate::Integer(*value), Some(ScalarType::Integer)))
        }
        Predicate::Text(value) => Ok((BoundPredicate::Text(value.clone()), Some(ScalarType::Text))),
        Predicate::Column(name) => {
            let column = resolve_column(columns, name)?;
            Ok((
                BoundPredicate::Column(column.into()),
                Some(column.scalar_type),
            ))
        }
        Predicate::Compare {
            operator,
            left,
            right,
        } => {
            let (left, left_type) = bind_predicate(left, columns)?;
            let (right, right_type) = bind_predicate(right, columns)?;
            if matches!((left_type, right_type), (Some(left), Some(right)) if left != right) {
                return Err(BindError::InvalidQuery);
            }
            Ok((
                BoundPredicate::Compare {
                    operator: *operator,
                    left: Box::new(left),
                    right: Box::new(right),
                },
                Some(ScalarType::Boolean),
            ))
        }
        Predicate::And(children) | Predicate::Or(children) => {
            if !(2..=16).contains(&children.len()) {
                return Err(BindError::InvalidQuery);
            }
            let mut bound = Vec::with_capacity(children.len());
            for child in children {
                let (child, scalar_type) = bind_predicate(child, columns)?;
                if scalar_type != Some(ScalarType::Boolean) {
                    return Err(BindError::InvalidQuery);
                }
                bound.push(child);
            }
            let node = match predicate {
                Predicate::And(_) => BoundPredicate::And(bound),
                Predicate::Or(_) => BoundPredicate::Or(bound),
                _ => return Err(BindError::InvalidQuery),
            };
            Ok((node, Some(ScalarType::Boolean)))
        }
        Predicate::Not(child) => {
            let (child, scalar_type) = bind_predicate(child, columns)?;
            if scalar_type != Some(ScalarType::Boolean) {
                return Err(BindError::InvalidQuery);
            }
            Ok((
                BoundPredicate::Not(Box::new(child)),
                Some(ScalarType::Boolean),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put_name(bytes: &mut Vec<u8>, name: &str) {
        bytes.push(name.len() as u8);
        bytes.extend_from_slice(name.as_bytes());
    }

    fn request(predicate: &[u8], projection: &str) -> Vec<u8> {
        let mut bytes = vec![0, 1, 1, 0];
        put_name(&mut bytes, "public");
        put_name(&mut bytes, "items");
        bytes.extend_from_slice(&1u16.to_be_bytes());
        put_name(&mut bytes, projection);
        bytes.extend_from_slice(&(predicate.len() as u32).to_be_bytes());
        bytes.extend_from_slice(predicate);
        bytes.push(0);
        bytes
    }

    fn relation() -> AuthorizedRelationDescription {
        AuthorizedRelationDescription {
            relation_id: [0x11; 16],
            name: "items".to_owned(),
            columns: vec![
                VisibleColumn {
                    column_id: [0x21; 16],
                    name: "id".to_owned(),
                    ordinal: 0,
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                },
                VisibleColumn {
                    column_id: [0x22; 16],
                    name: "active".to_owned(),
                    ordinal: 1,
                    scalar_type: ScalarType::Boolean,
                    nullable: true,
                },
            ],
        }
    }

    #[test]
    fn binds_only_authorized_relation_shape_and_stable_column_references() {
        let predicate = [0x20, 0, 0x10, 2, b'i', b'd', 0x02, 0, 0, 0, 0, 0, 0, 0, 7];
        let plan = bind_select_request(&request(&predicate, "ID"), Some(relation())).unwrap();
        assert_eq!(plan.relation_id, [0x11; 16]);
        assert_eq!(
            plan.projections,
            [BoundColumn {
                column_id: [0x21; 16],
                ordinal: 0,
                scalar_type: ScalarType::Integer,
                nullable: false,
            }]
        );
        assert_eq!(
            plan.predicate,
            Some(BoundPredicate::Compare {
                operator: Comparison::Equal,
                left: Box::new(BoundPredicate::Column(BoundColumn {
                    column_id: [0x21; 16],
                    ordinal: 0,
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                })),
                right: Box::new(BoundPredicate::Integer(7)),
            })
        );
    }

    #[test]
    fn relation_absence_and_name_mismatch_are_non_disclosing() {
        let bytes = request(&[], "id");
        assert_eq!(
            bind_select_request(&bytes, None),
            Err(BindError::RelationUnavailable)
        );
        let mut wrong_relation = relation();
        wrong_relation.name = "secret".to_owned();
        assert_eq!(
            bind_select_request(&bytes, Some(wrong_relation)),
            Err(BindError::RelationUnavailable)
        );
    }

    #[test]
    fn rejects_unknown_columns_and_type_incompatible_predicates() {
        let unknown_column = request(&[], "secret_column");
        assert_eq!(
            bind_select_request(&unknown_column, Some(relation())),
            Err(BindError::InvalidQuery)
        );
        let incompatible = [0x20, 0, 0x10, 2, b'i', b'd', 0x01, 1];
        assert_eq!(
            bind_select_request(&request(&incompatible, "id"), Some(relation())),
            Err(BindError::InvalidQuery)
        );
        let non_boolean_root = [0x10, 2, b'i', b'd'];
        assert_eq!(
            bind_select_request(&request(&non_boolean_root, "id"), Some(relation())),
            Err(BindError::InvalidQuery)
        );
    }
}
