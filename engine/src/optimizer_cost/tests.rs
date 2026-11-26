use super::*;
use crate::metadata_discovery::{
    AuthorizedPlanRelation, AuthorizedRangeAccessPath, AuthorizedRelationDescription,
    AuthorizedUniqueAccessPath, VisibleColumn,
};
use crate::optimizer_ir::bind::bind_logical_request;
use crate::optimizer_ir::expr::{ColumnRef, Comparison, Expr};
use crate::optimizer_ir::logical::{JoinType, LogicalOp, LogicalRequest, OrderItem, OrderTarget};
use crate::optimizer_ir::physical::PhysicalOp;
use crate::statistics::{AuthorizedColumnStats, AuthorizedRelationStats};
use crate::tuple::ScalarType;

fn test_relation(name: &str, id_byte: u8) -> AuthorizedPlanRelation {
    AuthorizedPlanRelation {
        schema_id: [1; 16],
        schema_version: 1,
        table_epoch: 1,
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
                    name: "payload".to_owned(),
                    ordinal: 2,
                    scalar_type: ScalarType::Text,
                    nullable: true,
                },
            ],
        },
        unique_access_paths: vec![AuthorizedUniqueAccessPath {
            index_id: [id_byte + 40; 16],
            column_ids: vec![[id_byte + 10; 16]],
        }],
        range_access_paths: vec![AuthorizedRangeAccessPath {
            index_id: [id_byte + 50; 16],
            column_ids: vec![[id_byte + 10; 16], [id_byte + 20; 16]],
        }],
    }
}

#[test]
fn and_conjunct_extracts_unique_probe_and_keeps_residual_filter() {
    let rel = test_relation("emp", 2);
    let req = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Filter {
            predicate: Expr::And(vec![
                Expr::Compare {
                    op: Comparison::Equal,
                    left: Box::new(Expr::Column(ColumnRef {
                        relation_index: 0,
                        column_name: "dept".to_owned(),
                    })),
                    right: Box::new(Expr::Text("eng".to_owned())),
                },
                Expr::Compare {
                    op: Comparison::Equal,
                    left: Box::new(Expr::Column(ColumnRef {
                        relation_index: 0,
                        column_name: "id".to_owned(),
                    })),
                    right: Box::new(Expr::Integer(42)),
                },
            ]),
            input: Box::new(LogicalOp::Scan {
                relation_index: 0,
                relation_name: "emp".to_owned(),
            }),
        },
    };
    let bound = bind_logical_request(&req, &[Some(rel)]).unwrap();
    let mut stats = StatsCatalog::default();
    stats.relations.insert(
        0,
        AuthorizedRelationStats {
            visible_rows: 10_000,
            is_exact: true,
            avg_authorized_row_bytes: 128,
            columns: vec![],
        },
    );
    let plan = optimize_bound_request(&bound, &stats, &CostWeights::default()).unwrap();
    match plan.root {
        PhysicalOp::Filter { input, .. } => {
            assert!(matches!(*input, PhysicalOp::IndexPointScan { .. }));
        }
        other => panic!("expected Filter over IndexPointScan, got {other:?}"),
    }
}

#[test]
fn wide_row_covering_index_chosen_over_seq_scan() {
    let rel = test_relation("emp", 2);
    let req = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Project {
            columns: vec![
                ColumnRef {
                    relation_index: 0,
                    column_name: "id".to_owned(),
                },
                ColumnRef {
                    relation_index: 0,
                    column_name: "dept".to_owned(),
                },
            ],
            input: Box::new(LogicalOp::Scan {
                relation_index: 0,
                relation_name: "emp".to_owned(),
            }),
        },
    };
    let bound = bind_logical_request(&req, &[Some(rel)]).unwrap();
    let mut stats = StatsCatalog::default();
    stats.relations.insert(
        0,
        AuthorizedRelationStats {
            visible_rows: 5_000,
            is_exact: true,
            avg_authorized_row_bytes: 512,
            columns: vec![AuthorizedColumnStats {
                column_id: [12; 16],
                ndv: Some(5_000),
                null_fraction_ppm: Some(0),
                redacted_fraction_ppm: 0,
                histogram: None,
            }],
        },
    );
    let plan = optimize_bound_request(&bound, &stats, &CostWeights::default()).unwrap();
    match plan.root {
        PhysicalOp::Project { input, .. } => {
            assert!(matches!(*input, PhysicalOp::CoveringIndexScan { .. }));
        }
        other => panic!("expected Project over CoveringIndexScan, got {other:?}"),
    }
}

#[test]
fn sort_limit_on_indexed_column_uses_index_ordered_scan() {
    let rel = test_relation("emp", 2);
    let req = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Limit {
            count: 10,
            input: Box::new(LogicalOp::Sort {
                order_by: vec![OrderItem {
                    target: OrderTarget::Column(ColumnRef {
                        relation_index: 0,
                        column_name: "id".to_owned(),
                    }),
                    descending: true,
                    nulls_first: false,
                }],
                input: Box::new(LogicalOp::Scan {
                    relation_index: 0,
                    relation_name: "emp".to_owned(),
                }),
            }),
        },
    };
    let bound = bind_logical_request(&req, &[Some(rel)]).unwrap();
    let plan =
        optimize_bound_request(&bound, &StatsCatalog::default(), &CostWeights::default()).unwrap();
    match plan.root {
        PhysicalOp::Limit { input, count: 10 } => {
            assert!(matches!(
                *input,
                PhysicalOp::IndexOrderedScan {
                    descending: true,
                    limit_hint: Some(10),
                    ..
                }
            ));
        }
        other => panic!("expected Limit over IndexOrderedScan, got {other:?}"),
    }
}

#[test]
fn constant_false_folds_to_empty_result() {
    let rel = test_relation("emp", 2);
    let req = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Filter {
            predicate: Expr::Compare {
                op: Comparison::Equal,
                left: Box::new(Expr::Integer(1)),
                right: Box::new(Expr::Integer(2)),
            },
            input: Box::new(LogicalOp::Scan {
                relation_index: 0,
                relation_name: "emp".to_owned(),
            }),
        },
    };
    let bound = bind_logical_request(&req, &[Some(rel)]).unwrap();
    let plan =
        optimize_bound_request(&bound, &StatsCatalog::default(), &CostWeights::default()).unwrap();
    assert!(matches!(plan.root, PhysicalOp::EmptyResult { .. }));
}

#[test]
fn join_prefers_index_nested_loop_for_skewed_authorized_stats() {
    let left = test_relation("small", 2);
    let right = test_relation("large", 3);
    let req = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Join {
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
                relation_name: "small".to_owned(),
            }),
            right: Box::new(LogicalOp::Scan {
                relation_index: 1,
                relation_name: "large".to_owned(),
            }),
        },
    };
    let bound = bind_logical_request(&req, &[Some(left), Some(right)]).unwrap();
    let mut stats = StatsCatalog::default();
    stats.relations.insert(
        0,
        AuthorizedRelationStats {
            visible_rows: 10,
            is_exact: true,
            avg_authorized_row_bytes: 32,
            columns: vec![],
        },
    );
    stats.relations.insert(
        1,
        AuthorizedRelationStats {
            visible_rows: 50_000,
            is_exact: true,
            avg_authorized_row_bytes: 256,
            columns: vec![],
        },
    );
    let plan = optimize_bound_request(&bound, &stats, &CostWeights::default()).unwrap();
    assert!(matches!(plan.root, PhysicalOp::IndexNestedLoopJoin { .. }));
}

#[test]
fn redacted_column_omits_ndv_and_preserves_residual_filter_noninterference() {
    let rel = test_relation("emp", 2);
    let req = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Filter {
            predicate: Expr::Compare {
                op: Comparison::Equal,
                left: Box::new(Expr::Column(ColumnRef {
                    relation_index: 0,
                    column_name: "dept".to_owned(),
                })),
                right: Box::new(Expr::Text("eng".to_owned())),
            },
            input: Box::new(LogicalOp::Scan {
                relation_index: 0,
                relation_name: "emp".to_owned(),
            }),
        },
    };
    let bound = bind_logical_request(&req, &[Some(rel)]).unwrap();

    let mut stats_a = StatsCatalog::default();
    stats_a.relations.insert(
        0,
        AuthorizedRelationStats {
            visible_rows: 500,
            is_exact: true,
            avg_authorized_row_bytes: 64,
            columns: vec![AuthorizedColumnStats {
                column_id: [22; 16],
                ndv: None,
                null_fraction_ppm: None,
                redacted_fraction_ppm: 500_000,
                histogram: None,
            }],
        },
    );
    let mut stats_b = stats_a.clone();
    stats_b.relations.get_mut(&0).unwrap().columns[0].redacted_fraction_ppm = 900_000;

    let plan_a = optimize_bound_request(&bound, &stats_a, &CostWeights::default()).unwrap();
    let plan_b = optimize_bound_request(&bound, &stats_b, &CostWeights::default()).unwrap();
    assert_eq!(plan_a, plan_b);
    assert!(matches!(plan_a.root, PhysicalOp::Filter { .. }));
}

#[test]
fn multi_way_three_table_join_roundtrips_and_validates() {
    let t1 = test_relation("t1", 2);
    let t2 = test_relation("t2", 3);
    let t3 = test_relation("t3", 4);
    let req = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Limit {
            count: 50,
            input: Box::new(LogicalOp::Join {
                join_type: JoinType::Inner,
                eq_keys: vec![(
                    ColumnRef {
                        relation_index: 1,
                        column_name: "id".to_owned(),
                    },
                    ColumnRef {
                        relation_index: 2,
                        column_name: "id".to_owned(),
                    },
                )],
                residual: None,
                left: Box::new(LogicalOp::Join {
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
                        relation_name: "t1".to_owned(),
                    }),
                    right: Box::new(LogicalOp::Scan {
                        relation_index: 1,
                        relation_name: "t2".to_owned(),
                    }),
                }),
                right: Box::new(LogicalOp::Scan {
                    relation_index: 2,
                    relation_name: "t3".to_owned(),
                }),
            }),
        },
    };
    let bound = bind_logical_request(&req, &[Some(t1), Some(t2), Some(t3)]).unwrap();
    let plan =
        optimize_bound_request(&bound, &StatsCatalog::default(), &CostWeights::default()).unwrap();
    let encoded = crate::optimizer_ir::encode_physical_plan(&plan).unwrap();
    let decoded = crate::optimizer_ir::decode_physical_plan(&encoded).unwrap();
    assert_eq!(plan, decoded);
}

#[test]
fn limit_zero_short_circuits_to_empty_result() {
    let rel = test_relation("emp", 2);
    let req = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Limit {
            count: 0,
            input: Box::new(LogicalOp::Scan {
                relation_index: 0,
                relation_name: "emp".to_owned(),
            }),
        },
    };
    let bound = bind_logical_request(&req, &[Some(rel)]).unwrap();
    let plan =
        optimize_bound_request(&bound, &StatsCatalog::default(), &CostWeights::default()).unwrap();
    assert!(matches!(plan.root, PhysicalOp::EmptyResult { .. }));
}

#[test]
fn unknown_stats_large_budget_sets_allow_spill_on_sort() {
    let rel = test_relation("emp", 2);
    let req = LogicalRequest {
        schema_name: "app".to_owned(),
        root: LogicalOp::Sort {
            order_by: vec![OrderItem {
                target: OrderTarget::Column(ColumnRef {
                    relation_index: 0,
                    column_name: "dept".to_owned(),
                }),
                descending: false,
                nulls_first: true,
            }],
            input: Box::new(LogicalOp::Scan {
                relation_index: 0,
                relation_name: "emp".to_owned(),
            }),
        },
    };
    let bound = bind_logical_request(&req, &[Some(rel)]).unwrap();
    let weights = CostWeights {
        in_memory_budget_bytes: 1024,
        ..CostWeights::default()
    };
    let plan = optimize_bound_request(&bound, &StatsCatalog::default(), &weights).unwrap();
    assert!(matches!(
        plan.root,
        PhysicalOp::Sort {
            allow_spill: true,
            ..
        }
    ));
}
