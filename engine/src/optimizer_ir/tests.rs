use super::*;
use crate::metadata_discovery::{
    AuthorizedPlanRelation, AuthorizedRelationDescription, AuthorizedUniqueAccessPath,
    VisibleColumn,
};
use crate::optimizer_ir::expr::{BoundColumnRef, ColumnRef};
use crate::optimizer_ir::logical::MAX_UNIFIED_REQUEST_BYTES;
use crate::optimizer_ir::physical::{MAX_UNIFIED_PLAN_BYTES, PhysicalOrderItem};
use crate::tuple::ScalarType;

fn sample_relation(name: &str, id_byte: u8) -> AuthorizedPlanRelation {
    AuthorizedPlanRelation {
        schema_id: [1; 16],
        schema_version: 10,
        table_epoch: 5,
        relation: AuthorizedRelationDescription {
            relation_id: [id_byte; 16],
            name: name.to_owned(),
            columns: vec![
                VisibleColumn {
                    column_id: [id_byte + 10; 16],
                    name: "id".to_owned(),
                    ordinal: 0,
                    scalar_type: ScalarType::Integer,
                    nullable: false,
                },
                VisibleColumn {
                    column_id: [id_byte + 20; 16],
                    name: "dept".to_owned(),
                    ordinal: 1,
                    scalar_type: ScalarType::Text,
                    nullable: true,
                },
                VisibleColumn {
                    column_id: [id_byte + 30; 16],
                    name: "salary".to_owned(),
                    ordinal: 2,
                    scalar_type: ScalarType::Decimal {
                        precision: 10,
                        scale: 2,
                    },
                    nullable: true,
                },
            ],
        },
        unique_access_paths: vec![AuthorizedUniqueAccessPath {
            index_id: [id_byte + 40; 16],
            column_ids: vec![[id_byte + 10; 16]],
        }],
        range_access_paths: vec![],
    }
}

#[test]
fn expr_roundtrip_and_bounds() {
    let expr = Expr::And(vec![
        Expr::Compare {
            op: Comparison::Equal,
            left: Box::new(Expr::Column(ColumnRef {
                relation_index: 0,
                column_name: "id".to_owned(),
            })),
            right: Box::new(Expr::Parameter(1)),
        },
        Expr::InList {
            expr: Box::new(Expr::Column(ColumnRef {
                relation_index: 0,
                column_name: "dept".to_owned(),
            })),
            items: vec![Expr::Text("eng".to_owned()), Expr::Text("ops".to_owned())],
            negated: false,
        },
        Expr::IsNull {
            expr: Box::new(Expr::Column(ColumnRef {
                relation_index: 0,
                column_name: "salary".to_owned(),
            })),
            negated: true,
        },
    ]);
    let encoded = encode_expr(&expr).expect("encode expr");
    let decoded = decode_expr(&encoded).expect("decode expr");
    assert_eq!(expr, decoded);

    let mut with_trailing = encoded.clone();
    with_trailing.push(0);
    assert_eq!(decode_expr(&with_trailing), Err(ExprError::TrailingBytes));

    let truncated = &encoded[..encoded.len() - 1];
    assert_eq!(decode_expr(truncated), Err(ExprError::Truncated));

    let mut deep = Expr::Boolean(true);
    for _ in 0..MAX_EXPR_DEPTH {
        deep = Expr::Not(Box::new(deep));
    }
    assert_eq!(encode_expr(&deep), Err(ExprError::TooDeep));
}

#[test]
fn logical_request_roundtrip_multi_join_agg_sort_limit() {
    let req = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Limit {
            count: 25,
            input: Box::new(LogicalOp::Sort {
                order_by: vec![OrderItem {
                    target: OrderTarget::AggregateIndex(0),
                    descending: true,
                    nulls_first: false,
                }],
                input: Box::new(LogicalOp::Aggregate {
                    group_keys: vec![ColumnRef {
                        relation_index: 0,
                        column_name: "dept".to_owned(),
                    }],
                    aggregates: vec![AggregateSpec {
                        kind: AggregateKind::Sum,
                        arg: Some(ColumnRef {
                            relation_index: 1,
                            column_name: "salary".to_owned(),
                        }),
                    }],
                    having: Some(Expr::Compare {
                        op: Comparison::Greater,
                        left: Box::new(Expr::Column(ColumnRef {
                            relation_index: 0,
                            column_name: "id".to_owned(),
                        })),
                        right: Box::new(Expr::Integer(0)),
                    }),
                    input: Box::new(LogicalOp::Join {
                        join_type: JoinType::Inner,
                        eq_keys: vec![(
                            ColumnRef {
                                relation_index: 0,
                                column_name: "id".to_owned(),
                            },
                            ColumnRef {
                                relation_index: 1,
                                column_name: "id".to_owned(),
                            },
                        )],
                        residual: None,
                        left: Box::new(LogicalOp::Scan {
                            relation_index: 0,
                            relation_name: "employees".to_owned(),
                        }),
                        right: Box::new(LogicalOp::Scan {
                            relation_index: 1,
                            relation_name: "salaries".to_owned(),
                        }),
                    }),
                }),
            }),
        },
    };

    let bytes = encode_logical_request(&req).expect("encode logical request");
    assert!(bytes.len() <= MAX_UNIFIED_REQUEST_BYTES);
    let decoded = decode_logical_request(&bytes).expect("decode logical request");
    assert_eq!(req, decoded);

    let relations = collect_scan_relations(&decoded);
    assert_eq!(
        relations,
        vec![
            ("app".to_owned(), "employees".to_owned()),
            ("app".to_owned(), "salaries".to_owned()),
        ]
    );

    let bound = bind_logical_request(
        &decoded,
        &[
            Some(sample_relation("employees", 2)),
            Some(sample_relation("salaries", 3)),
        ],
    )
    .expect("bind multi-join aggregate");
    assert_eq!(bound.schema_version, 10);
}

#[test]
fn binder_rejects_non_nullable_update_null_and_non_numeric_sum() {
    let bad_update = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Update {
            relation_index: 0,
            relation_name: "employees".to_owned(),
            assignments: vec![UpdateAssignment {
                column_name: "id".to_owned(),
                value: UpdateValue::Null,
            }],
            predicate: Expr::Boolean(true),
        },
    };
    assert_eq!(
        bind_logical_request(&bad_update, &[Some(sample_relation("employees", 2))]),
        Err(BindError::InvalidQuery)
    );

    let bad_sum = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Aggregate {
            input: Box::new(LogicalOp::Scan {
                relation_index: 0,
                relation_name: "employees".to_owned(),
            }),
            group_keys: vec![],
            aggregates: vec![AggregateSpec {
                kind: AggregateKind::Sum,
                arg: Some(ColumnRef {
                    relation_index: 0,
                    column_name: "dept".to_owned(),
                }),
            }],
            having: None,
        },
    };
    assert_eq!(
        bind_logical_request(&bad_sum, &[Some(sample_relation("employees", 2))]),
        Err(BindError::InvalidQuery)
    );
}

#[test]
fn physical_plan_roundtrip_and_validation() {
    let rel = sample_relation("employees", 2);
    let col_id = BoundColumn {
        column_id: rel.relation.columns[0].column_id,
        ordinal: 0,
        scalar_type: ScalarType::Integer,
        nullable: false,
    };
    let col_dept = BoundColumn {
        column_id: rel.relation.columns[1].column_id,
        ordinal: 1,
        scalar_type: ScalarType::Text,
        nullable: true,
    };

    let plan = PhysicalPlan {
        schema_name: "app".to_owned(),
        schema_id: [1; 16],
        schema_version: 10,
        root: PhysicalOp::TopNHeap {
            limit: 10,
            order_by: vec![PhysicalOrderItem {
                target: BoundOrderTarget::Column(BoundColumnRef {
                    relation_index: 0,
                    column: col_id.clone(),
                }),
                descending: true,
                nulls_first: false,
            }],
            input: Box::new(PhysicalOp::Project {
                columns: vec![col_id.clone(), col_dept.clone()],
                input: Box::new(PhysicalOp::Filter {
                    predicate: BoundExpr::Compare {
                        op: Comparison::Equal,
                        left: Box::new(BoundExpr::Column(BoundColumnRef {
                            relation_index: 0,
                            column: col_dept.clone(),
                        })),
                        right: Box::new(BoundExpr::Text("eng".to_owned())),
                    },
                    input: Box::new(PhysicalOp::IndexOrderedScan {
                        relation: RelationReference {
                            relation_index: 0,
                            relation_name: "employees".to_owned(),
                            relation_id: rel.relation.relation_id,
                            table_epoch: 5,
                        },
                        scan_columns: vec![col_id.clone(), col_dept.clone()],
                        index_id: rel.unique_access_paths[0].index_id,
                        order_columns: vec![col_id],
                        descending: true,
                        limit_hint: Some(10),
                    }),
                }),
            }),
        },
    };

    let bytes = encode_physical_plan(&plan).expect("encode physical plan");
    assert!(bytes.len() <= MAX_UNIFIED_PLAN_BYTES);
    let decoded = decode_physical_plan(&bytes).expect("decode physical plan");
    assert_eq!(plan, decoded);
}
