//! Invocation-bound structured query planning service.

use crate::metadata_discovery::AuthorizedPlanRelation;
use crate::optimizer_ir::{BindError, PlanError, RequestError};
use std::fmt;

/// Failure returned to the caller of the optimizer service. Relation lookup
/// failures intentionally do not expose whether a name exists outside the
/// invocation's authorized metadata view.
#[derive(Debug)]
pub enum OptimizerError {
    Request(RequestError),
    Bind(BindError),
    Plan(PlanError),
}

impl fmt::Display for OptimizerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "optimizer service error: {self:?}")
    }
}

impl std::error::Error for OptimizerError {}

impl From<RequestError> for OptimizerError {
    fn from(error: RequestError) -> Self {
        Self::Request(error)
    }
}

impl From<BindError> for OptimizerError {
    fn from(error: BindError) -> Self {
        Self::Bind(error)
    }
}

impl From<PlanError> for OptimizerError {
    fn from(error: PlanError) -> Self {
        Self::Plan(error)
    }
}

/// Optimize a bound `LogicalRequest` using pre-fetched `AuthorizedPlanRelation`s
/// and optional authorized-view `StatsCatalog`.
pub fn optimize_unified_v1_with_relations(
    request: &[u8],
    relations: &[Option<AuthorizedPlanRelation>],
    stats: &crate::optimizer_cost::StatsCatalog,
    weights: &crate::optimizer_cost::CostWeights,
) -> Result<Vec<u8>, OptimizerError> {
    let decoded = crate::optimizer_ir::decode_logical_request(request)?;
    let bound = crate::optimizer_ir::bind_logical_request(&decoded, relations)?;
    let plan = crate::optimizer_cost::optimize_bound_request(&bound, stats, weights)
        .map_err(|_| OptimizerError::Plan(PlanError::InvalidPipeline))?;
    Ok(crate::optimizer_ir::encode_physical_plan(&plan)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_discovery::{AuthorizedRelationDescription, VisibleColumn};
    use crate::optimizer_cost::{CostWeights, StatsCatalog};
    use crate::optimizer_ir::expr::ColumnRef;
    use crate::optimizer_ir::{
        Comparison, Expr, LogicalOp, LogicalRequest, PhysicalOp, decode_physical_plan,
        encode_logical_request,
    };
    use crate::tuple::ScalarType;

    fn authorized_relation(name: &str) -> AuthorizedPlanRelation {
        AuthorizedPlanRelation {
            schema_id: [0x11; 16],
            schema_version: 1,
            table_epoch: 1,
            relation: AuthorizedRelationDescription {
                relation_id: [0x22; 16],
                name: name.to_owned(),
                columns: vec![
                    VisibleColumn {
                        column_id: [0x31; 16],
                        ordinal: 0,
                        name: "id".to_owned(),
                        scalar_type: ScalarType::Integer,
                        nullable: false,
                    },
                    VisibleColumn {
                        column_id: [0x32; 16],
                        ordinal: 1,
                        name: "score".to_owned(),
                        scalar_type: ScalarType::Integer,
                        nullable: true,
                    },
                ],
            },
            unique_access_paths: Vec::new(),
            range_access_paths: Vec::new(),
        }
    }

    #[test]
    fn unified_service_optimizes_filtered_select_and_hides_missing_relations() {
        let req = LogicalRequest {
            schema_name: "app".to_owned(),
            root: LogicalOp::Project {
                columns: vec![ColumnRef {
                    relation_index: 0,
                    column_name: "score".to_owned(),
                }],
                input: Box::new(LogicalOp::Filter {
                    predicate: Expr::Compare {
                        op: Comparison::Equal,
                        left: Box::new(Expr::Column(ColumnRef {
                            relation_index: 0,
                            column_name: "id".to_owned(),
                        })),
                        right: Box::new(Expr::Integer(7)),
                    },
                    input: Box::new(LogicalOp::Scan {
                        relation_index: 0,
                        relation_name: "records".to_owned(),
                    }),
                }),
            },
        };
        let encoded_req = encode_logical_request(&req).unwrap();
        let encoded_plan = optimize_unified_v1_with_relations(
            &encoded_req,
            &[Some(authorized_relation("records"))],
            &StatsCatalog::default(),
            &CostWeights::default(),
        )
        .unwrap();
        let decoded = decode_physical_plan(&encoded_plan).unwrap();
        assert!(matches!(decoded.root, PhysicalOp::Project { .. }));

        let missing = optimize_unified_v1_with_relations(
            &encoded_req,
            &[None],
            &StatsCatalog::default(),
            &CostWeights::default(),
        );
        assert!(matches!(
            missing,
            Err(OptimizerError::Bind(BindError::RelationUnavailable))
        ));
    }

    #[test]
    fn unified_service_optimizes_composite_group_limit_wire_request() {
        let req_bytes: [u8; 71] = [
            0x00, 0x01, 0x80, 0x00, 0x0d, b's', b'q', b'l', b'_', b'r', b'o', b'u', b'n', b'd',
            b't', b'r', b'i', b'p', 0x07, 0x00, 0x00, 0x00, 0x02, 0x05, 0x02, 0x00, 0x00, 0x08,
            b'l', b'e', b'f', b't', b'_', b'k', b'e', b'y', 0x00, 0x00, 0x09, b'r', b'i', b'g',
            b'h', b't', b'_', b'k', b'e', b'y', 0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x0f,
            b'g', b'r', b'o', b'u', b'p', b'e', b'd', b'_', b'r', b'e', b'c', b'o', b'r', b'd',
            b's',
        ];
        let mut rel = authorized_relation("grouped_records");
        rel.relation.columns[0].name = "left_key".to_owned();
        rel.relation.columns[1].name = "right_key".to_owned();
        let plan_bytes = optimize_unified_v1_with_relations(
            &req_bytes,
            &[Some(rel)],
            &StatsCatalog::default(),
            &CostWeights::default(),
        )
        .expect("composite group limit should optimize");
        let plan = decode_physical_plan(&plan_bytes).expect("decode physical plan");
        assert!(matches!(plan.root, PhysicalOp::Limit { count: 2, .. }));
    }
}
