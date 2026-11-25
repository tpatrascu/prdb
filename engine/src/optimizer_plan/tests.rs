use super::select_join::*;
use super::types::*;
use super::validate::*;
use super::wire::*;
use super::*;
use crate::optimizer_binding::{
    BoundColumn, BoundCompositeGroupAggregate, BoundCompositeGroupCount,
    BoundFilteredCompositeGroupAggregate, BoundGroupAggregate, BoundGroupCount, BoundJoinFilter,
    BoundOrderedCompositeGroupAggregate, BoundOrderedCompositeGroupCount,
    BoundOrderedGroupAggregate, BoundOrderedGroupCount, BoundOrderedSelect, BoundPredicate,
    BoundSelect, BoundUpdateAssignment, BoundUpdateValue,
};
use crate::optimizer_request::{
    CompositeGroupCountOrderItem, GroupCountOrderItem, JoinRelationSide,
};
use crate::tuple::ScalarType;

use crate::optimizer_request::Comparison;

fn column(id: u8, ordinal: u32, scalar_type: ScalarType) -> BoundColumn {
    BoundColumn {
        column_id: [id; 16],
        ordinal,
        scalar_type,
        nullable: false,
    }
}

fn bound_select() -> BoundSelect {
    let output = column(1, 0, ScalarType::Integer);
    let filter = column(2, 1, ScalarType::Boolean);
    BoundSelect {
        schema_name: "sales".to_owned(),
        schema_id: [0x10; 16],
        schema_version: 4,
        table_epoch: 6,
        relation_id: [0x20; 16],
        relation_name: "orders".to_owned(),
        projections: vec![output.clone()],
        predicate: Some(BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Column(filter)),
            right: Box::new(BoundPredicate::Boolean(true)),
        }),
        unique_access_paths: Vec::new(),
        range_access_paths: Vec::new(),
        limit: Some(5),
    }
}

#[test]
fn same_key_or_equality_plan_uses_a_batched_probe_hint() {
    let key = column(7, 1, ScalarType::Integer);
    let equality = |value| BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(key.clone())),
        right: Box::new(BoundPredicate::Integer(value)),
    };
    let mut bound = bound_select();
    bound.predicate = Some(BoundPredicate::Or(vec![
        equality(11),
        equality(22),
        BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Column(key.clone())),
            right: Box::new(BoundPredicate::Null),
        },
    ]));
    bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
        index_id: [0x71; 16],
        column_ids: vec![key.column_id],
    }];

    let plan = plan_select_v1(bound).unwrap();
    let probe = plan.multi_point_probe.as_ref().unwrap();
    assert_eq!(probe.index_id, [0x71; 16]);
    assert_eq!(probe.key_column, key);
    assert_eq!(
        probe.values,
        vec![BoundPredicate::Integer(11), BoundPredicate::Integer(22)]
    );
    assert_eq!(
        decode_plan_v1(&encode_plan_v1(&plan).unwrap()).unwrap(),
        plan
    );
    let mut malformed = encode_plan_v1(&plan).unwrap();
    *malformed.last_mut().unwrap() ^= 1;
    assert_eq!(decode_plan_v1(&malformed), Err(PlanError::InvalidPredicate));
}

#[test]
fn same_key_or_with_one_non_null_value_keeps_a_single_probe_hint() {
    let key = column(7, 1, ScalarType::Integer);
    let mut bound = bound_select();
    bound.predicate = Some(BoundPredicate::Or(vec![
        BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Column(key.clone())),
            right: Box::new(BoundPredicate::Integer(11)),
        },
        BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Column(key.clone())),
            right: Box::new(BoundPredicate::Null),
        },
    ]));
    bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
        index_id: [0x71; 16],
        column_ids: vec![key.column_id],
    }];

    let plan = plan_select_v1(bound).unwrap();
    let probe = plan.multi_point_probe.as_ref().unwrap();
    assert_eq!(probe.values, vec![BoundPredicate::Integer(11)]);
    assert_eq!(
        decode_plan_v1(&encode_plan_v1(&plan).unwrap()).unwrap(),
        plan
    );
}

#[test]
fn repeated_or_equalities_use_one_distinct_multi_point_probe() {
    let key = column(7, 1, ScalarType::Integer);
    let equality = || BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(key.clone())),
        right: Box::new(BoundPredicate::Integer(11)),
    };
    let mut bound = bound_select();
    bound.predicate = Some(BoundPredicate::Or(vec![equality(), equality()]));
    bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
        index_id: [0x71; 16],
        column_ids: vec![key.column_id],
    }];

    let plan = plan_select_v1(bound).unwrap();
    assert_eq!(
        plan.multi_point_probe.unwrap().values,
        vec![BoundPredicate::Integer(11)]
    );
}

#[test]
fn ordered_select_roundtrips_hidden_sort_key_and_limit() {
    let output = column(1, 0, ScalarType::Integer);
    let sort_key = column(2, 1, ScalarType::Boolean);
    let bound = BoundOrderedSelect {
        schema_name: "sales".to_owned(),
        schema_id: [0x10; 16],
        schema_version: 4,
        table_epoch: 6,
        relation_id: [0x20; 16],
        relation_name: "orders".to_owned(),
        projections: vec![output.clone()],
        order_key: sort_key.clone(),
        descending: true,
        limit: Some(5),
    };
    let plan = plan_ordered_select_v1(bound).unwrap();
    assert_eq!(plan.scan_columns, [output, sort_key.clone()]);
    assert_eq!(plan.order_key, sort_key);
    assert!(plan.descending);
    assert_eq!(plan.limit, Some(5));

    let bytes = encode_ordered_select_plan_v1(&plan).unwrap();
    assert_eq!(decode_ordered_select_plan_v1(&bytes).unwrap(), plan);
    let mut malformed = plan;
    malformed.scan_columns.pop();
    assert_eq!(
        encode_ordered_select_plan_v1(&malformed),
        Err(PlanError::InvalidColumnReference)
    );
}

#[test]
fn lowers_filter_columns_into_scan_and_preserves_operator_order() {
    let plan = plan_select_v1(bound_select()).unwrap();
    assert_eq!(
        plan.relation,
        RelationReference {
            schema_name: "sales".to_owned(),
            schema_id: [0x10; 16],
            schema_version: 4,
            relation_name: "orders".to_owned(),
            relation_id: [0x20; 16],
            table_epoch: 6,
        }
    );
    assert_eq!(
        plan.nodes,
        [
            PlanNode::Scan {
                columns: vec![
                    column(1, 0, ScalarType::Integer),
                    column(2, 1, ScalarType::Boolean),
                ],
            },
            PlanNode::Filter(BoundPredicate::Compare {
                operator: Comparison::Equal,
                left: Box::new(BoundPredicate::Column(column(2, 1, ScalarType::Boolean))),
                right: Box::new(BoundPredicate::Boolean(true)),
            }),
            PlanNode::Project(vec![column(1, 0, ScalarType::Integer)]),
            PlanNode::Limit(5),
        ]
    );
    assert!(plan.unique_probe.is_none());
    assert_eq!(validate_plan_v1(&plan), Ok(()));
}

#[test]
fn unique_point_hint_requires_an_approved_exact_equality_path() {
    let key = column(2, 1, ScalarType::Boolean);
    let mut bound = bound_select();
    bound.unique_access_paths = vec![crate::metadata_discovery::AuthorizedUniqueAccessPath {
        index_id: [0x44; 16],
        column_ids: vec![key.column_id],
    }];
    let plan = plan_select_v1(bound.clone()).unwrap();
    assert_eq!(
        plan.unique_probe,
        Some(UniquePointProbeV1 {
            index_id: [0x44; 16],
            key_column: key.clone(),
            value: BoundPredicate::Boolean(true),
        })
    );
    let mut small_population = plan.clone();
    assert!(apply_population_scan_heuristic(&mut small_population, Some(4)).unwrap());
    assert!(small_population.unique_probe.is_none());
    let mut unknown_population = plan.clone();
    assert!(!apply_population_scan_heuristic(&mut unknown_population, None).unwrap());
    assert!(unknown_population.unique_probe.is_some());
    let mut broad_match = plan.clone();
    assert!(
        !apply_range_selectivity_scan_heuristic(&mut broad_match, Some(130), Some(120),).unwrap()
    );
    assert!(broad_match.unique_probe.is_some());
    let encoded = encode_plan_v1(&plan).unwrap();
    assert_eq!(decode_plan_v1(&encoded).unwrap(), plan);

    bound.unique_access_paths.clear();
    assert!(
        plan_select_v1(bound.clone())
            .unwrap()
            .unique_probe
            .is_none()
    );
    bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
        index_id: [0x46; 16],
        column_ids: vec![key.column_id],
    }];
    assert!(
        plan_select_v1(bound.clone())
            .unwrap()
            .unique_probe
            .is_none()
    );
    bound.unique_access_paths = vec![crate::metadata_discovery::AuthorizedUniqueAccessPath {
        index_id: [0x44; 16],
        column_ids: vec![key.column_id, [0x45; 16]],
    }];
    assert!(plan_select_v1(bound).unwrap().unique_probe.is_none());

    let integer_key = column(2, 1, ScalarType::Integer);
    let mut out_of_range = bound_select();
    out_of_range.predicate = Some(BoundPredicate::Compare {
        operator: crate::optimizer_request::Comparison::Equal,
        left: Box::new(BoundPredicate::Column(integer_key.clone())),
        right: Box::new(BoundPredicate::Integer(i64::MAX)),
    });
    out_of_range.unique_access_paths =
        vec![crate::metadata_discovery::AuthorizedUniqueAccessPath {
            index_id: [0x44; 16],
            column_ids: vec![integer_key.column_id],
        }];
    assert!(plan_select_v1(out_of_range).unwrap().unique_probe.is_none());
}

#[test]
fn bounded_range_hint_roundtrips_only_for_an_approved_leading_key() {
    let key = column(3, 1, ScalarType::Integer);
    let mut bound = bound_select();
    bound.predicate = Some(BoundPredicate::Compare {
        operator: Comparison::GreaterOrEqual,
        left: Box::new(BoundPredicate::Column(key.clone())),
        right: Box::new(BoundPredicate::Integer(42)),
    });
    bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
        index_id: [0x46; 16],
        column_ids: vec![key.column_id, [0x47; 16]],
    }];
    let plan = plan_select_v1(bound.clone()).unwrap();
    assert_eq!(plan.unique_probe, None);
    assert_eq!(
        plan.range_scan,
        Some(IndexRangeScanV1 {
            index_id: [0x46; 16],
            key_column: key.clone(),
            lower: Some(IndexRangeBoundV1 {
                value: BoundPredicate::Integer(42),
                inclusive: true,
            }),
            upper: None,
        })
    );
    let mut small_population = plan.clone();
    assert!(apply_population_scan_heuristic(&mut small_population, Some(4)).unwrap());
    assert!(small_population.range_scan.is_none());
    assert!(!apply_population_scan_heuristic(&mut small_population, Some(4)).unwrap());
    let mut unknown_population = plan.clone();
    assert!(!apply_population_scan_heuristic(&mut unknown_population, None).unwrap());
    assert!(unknown_population.range_scan.is_some());
    let mut larger_population = plan.clone();
    assert!(!apply_population_scan_heuristic(&mut larger_population, Some(5)).unwrap());
    assert!(larger_population.range_scan.is_some());
    assert!(
        !apply_range_selectivity_scan_heuristic(&mut larger_population, Some(130), Some(64),)
            .unwrap()
    );
    assert!(larger_population.range_scan.is_some());
    assert!(
        apply_range_selectivity_scan_heuristic(&mut larger_population, Some(130), Some(65),)
            .unwrap()
    );
    assert!(larger_population.range_scan.is_none());
    assert!(matches!(
        apply_range_selectivity_scan_heuristic(&mut plan.clone(), Some(130), Some(131)),
        Err(PlanError::InvalidPredicate)
    ));
    let encoded = encode_plan_v1(&plan).unwrap();
    assert_eq!(decode_plan_v1(&encoded).unwrap(), plan);

    bound.range_access_paths.clear();
    assert!(plan_select_v1(bound.clone()).unwrap().range_scan.is_none());
    bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
        index_id: [0x46; 16],
        column_ids: vec![[0x48; 16], key.column_id],
    }];
    assert!(plan_select_v1(bound.clone()).unwrap().range_scan.is_none());

    let mut mismatched = plan;
    let Some(PlanNode::Filter(BoundPredicate::Compare { right, .. })) = mismatched.nodes.get_mut(1)
    else {
        panic!("range plan must retain its predicate");
    };
    **right = BoundPredicate::Integer(43);
    assert_eq!(
        validate_plan_v1(&mismatched),
        Err(PlanError::InvalidPredicate)
    );
}

#[test]
fn non_unique_range_hint_uses_equal_inclusive_bounds_for_equality() {
    let key = column(3, 1, ScalarType::Integer);
    let mut bound = bound_select();
    bound.predicate = Some(BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Integer(42)),
        right: Box::new(BoundPredicate::Column(key.clone())),
    });
    bound.unique_access_paths.clear();
    bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
        index_id: [0x46; 16],
        column_ids: vec![key.column_id, [0x47; 16]],
    }];

    let plan = plan_select_v1(bound).unwrap();
    let bound = IndexRangeBoundV1 {
        value: BoundPredicate::Integer(42),
        inclusive: true,
    };
    assert_eq!(
        plan.range_scan,
        Some(IndexRangeScanV1 {
            index_id: [0x46; 16],
            key_column: key,
            lower: Some(bound.clone()),
            upper: Some(bound),
        })
    );
    assert_eq!(
        decode_plan_v1(&encode_plan_v1(&plan).unwrap()).unwrap(),
        plan
    );
}

#[test]
fn bounded_range_hint_accepts_a_two_sided_same_key_conjunction() {
    let key = column(3, 1, ScalarType::Integer);
    let mut bound = bound_select();
    bound.predicate = Some(BoundPredicate::And(vec![
        BoundPredicate::Compare {
            operator: Comparison::LessOrEqual,
            left: Box::new(BoundPredicate::Integer(10)),
            right: Box::new(BoundPredicate::Column(key.clone())),
        },
        BoundPredicate::Compare {
            operator: Comparison::Less,
            left: Box::new(BoundPredicate::Column(key.clone())),
            right: Box::new(BoundPredicate::Integer(20)),
        },
    ]));
    bound.range_access_paths = vec![crate::metadata_discovery::AuthorizedRangeAccessPath {
        index_id: [0x49; 16],
        column_ids: vec![key.column_id, [0x4a; 16]],
    }];

    let plan = plan_select_v1(bound).unwrap();
    assert_eq!(
        plan.range_scan,
        Some(IndexRangeScanV1 {
            index_id: [0x49; 16],
            key_column: key,
            lower: Some(IndexRangeBoundV1 {
                value: BoundPredicate::Integer(10),
                inclusive: true,
            }),
            upper: Some(IndexRangeBoundV1 {
                value: BoundPredicate::Integer(20),
                inclusive: false,
            }),
        })
    );
    assert_eq!(
        decode_plan_v1(&encode_plan_v1(&plan).unwrap()).unwrap(),
        plan
    );
    let mut mismatched = plan.clone();
    let Some(PlanNode::Filter(BoundPredicate::And(children))) = mismatched.nodes.get_mut(1) else {
        panic!("two-sided range plan must retain its conjunction");
    };
    let Some(BoundPredicate::Compare { right, .. }) = children.get_mut(1) else {
        panic!("upper range term must retain its comparison");
    };
    **right = BoundPredicate::Integer(21);
    assert_eq!(
        validate_plan_v1(&mismatched),
        Err(PlanError::InvalidPredicate)
    );
}

#[test]
fn builds_and_roundtrips_a_bounded_delete_plan() {
    let bound = bound_select();
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let predicate = BoundPredicate::And(vec![
        BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Column(column(2, 1, ScalarType::Boolean))),
            right: Box::new(BoundPredicate::Boolean(true)),
        },
        BoundPredicate::Compare {
            operator: Comparison::Equal,
            left: Box::new(BoundPredicate::Column(column(1, 0, ScalarType::Integer))),
            right: Box::new(BoundPredicate::Integer(7)),
        },
    ]);

    let plan = plan_delete_v1(relation, predicate).unwrap();
    assert_eq!(
        plan.scan_columns,
        vec![
            column(1, 0, ScalarType::Integer),
            column(2, 1, ScalarType::Boolean),
        ]
    );
    assert_eq!(
        validate_delete_plan_v1(&plan),
        Ok(()),
        "the plan is valid before crossing the optimizer boundary"
    );
    let encoded = encode_delete_plan_v1(&plan).unwrap();
    assert_eq!(decode_delete_plan_v1(&encoded).unwrap(), plan);

    let mut malformed = encoded;
    *malformed.last_mut().unwrap() = 4;
    assert_eq!(
        decode_delete_plan_v1(&malformed),
        Err(PlanError::InvalidPipeline)
    );
}

#[test]
fn delete_plan_supports_constant_predicates_with_full_shape_scans() {
    let bound = bound_select();
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let plan = plan_delete_v1(relation, BoundPredicate::Boolean(true)).unwrap();
    assert!(plan.scan_columns.is_empty());
    let encoded = encode_delete_plan_v1(&plan).unwrap();
    let mut expected = vec![0x00, 0x01, PLAN_KIND_DELETE, 0x00];
    expected.extend_from_slice(&[5]);
    expected.extend_from_slice(b"sales");
    expected.extend_from_slice(&[0x10; 16]);
    expected.extend_from_slice(&4u64.to_be_bytes());
    expected.extend_from_slice(&[6]);
    expected.extend_from_slice(b"orders");
    expected.extend_from_slice(&[0x20; 16]);
    expected.extend_from_slice(&6u64.to_be_bytes());
    expected.extend_from_slice(&[3, 1, 0, 0, 2, 0x01, 0x01, DELETE_SINK_NODE]);
    assert_eq!(encoded, expected);
    assert_eq!(decode_delete_plan_v1(&encoded).unwrap(), plan);
}

#[test]
fn delete_plan_rejects_non_boolean_and_unbounded_predicates() {
    let bound = bound_select();
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    assert_eq!(
        plan_delete_v1(relation.clone(), BoundPredicate::Integer(7)),
        Err(PlanError::InvalidPredicate)
    );

    let mut predicate = BoundPredicate::Boolean(true);
    for _ in 0..crate::optimizer_request::MAX_PREDICATE_DEPTH {
        predicate = BoundPredicate::Not(Box::new(predicate));
    }
    assert_eq!(
        plan_delete_v1(relation, predicate),
        Err(PlanError::InvalidPredicate)
    );
}

#[test]
fn builds_and_roundtrips_a_bounded_update_plan() {
    let bound = bound_select();
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let active = BoundColumn {
        nullable: true,
        ..column(2, 1, ScalarType::Boolean)
    };
    let predicate = BoundPredicate::Compare {
        operator: Comparison::Greater,
        left: Box::new(BoundPredicate::Column(column(1, 0, ScalarType::Integer))),
        right: Box::new(BoundPredicate::Integer(7)),
    };
    let assignments = vec![
        BoundUpdateAssignment {
            column: active.clone(),
            value: BoundUpdateValue::Parameter(2),
        },
        BoundUpdateAssignment {
            column: column(1, 0, ScalarType::Integer),
            value: BoundUpdateValue::Parameter(1),
        },
    ];
    let plan = plan_update_v1(relation, predicate, assignments).unwrap();
    assert_eq!(
        plan.scan_columns,
        vec![column(1, 0, ScalarType::Integer), active.clone()]
    );
    assert_eq!(validate_update_plan_v1(&plan), Ok(()));
    let encoded = encode_update_plan_v1(&plan).unwrap();
    assert_eq!(decode_update_plan_v1(&encoded).unwrap(), plan);

    let mut trailing = encoded.clone();
    trailing.push(0);
    assert_eq!(
        decode_update_plan_v1(&trailing),
        Err(PlanError::TrailingBytes)
    );
    let mut invalid_assignment_kind = encoded;
    let invalid_kind_offset = invalid_assignment_kind.len() - 5;
    invalid_assignment_kind[invalid_kind_offset] = 9;
    assert_eq!(
        decode_update_plan_v1(&invalid_assignment_kind),
        Err(PlanError::InvalidEncoding)
    );
}

#[test]
fn update_plan_rejects_invalid_assignments_and_scan_shapes() {
    let bound = bound_select();
    let relation = RelationReference {
        schema_name: bound.schema_name,
        schema_id: bound.schema_id,
        schema_version: bound.schema_version,
        relation_name: bound.relation_name,
        relation_id: bound.relation_id,
        table_epoch: bound.table_epoch,
    };
    let predicate = BoundPredicate::Boolean(true);
    let required = column(1, 0, ScalarType::Integer);
    assert_eq!(
        plan_update_v1(
            relation.clone(),
            predicate.clone(),
            vec![BoundUpdateAssignment {
                column: required.clone(),
                value: BoundUpdateValue::Null,
            }],
        ),
        Err(PlanError::InvalidEncoding)
    );
    assert_eq!(
        plan_update_v1(
            relation.clone(),
            predicate.clone(),
            vec![BoundUpdateAssignment {
                column: required.clone(),
                value: BoundUpdateValue::Parameter(0),
            }],
        ),
        Err(PlanError::InvalidEncoding)
    );
    let valid_assignment = BoundUpdateAssignment {
        column: required.clone(),
        value: BoundUpdateValue::Parameter(1),
    };
    assert_eq!(
        plan_update_v1(
            relation.clone(),
            predicate.clone(),
            vec![valid_assignment.clone(), valid_assignment],
        ),
        Err(PlanError::InvalidColumnReference)
    );

    let mut invalid = plan_update_v1(
        relation,
        predicate,
        vec![BoundUpdateAssignment {
            column: required,
            value: BoundUpdateValue::Parameter(1),
        }],
    )
    .unwrap();
    invalid.scan_columns.clear();
    assert_eq!(
        validate_update_plan_v1(&invalid),
        Err(PlanError::InvalidColumnReference)
    );
}

#[test]
fn rejects_noncanonical_operator_order_and_duplicate_limits() {
    let mut plan = plan_select_v1(bound_select()).unwrap();
    plan.nodes.swap(1, 2);
    assert_eq!(validate_plan_v1(&plan), Err(PlanError::InvalidPipeline));

    let mut plan = plan_select_v1(bound_select()).unwrap();
    plan.nodes.pop();
    plan.nodes.push(PlanNode::Limit(5));
    plan.nodes.push(PlanNode::Limit(9));
    assert_eq!(validate_plan_v1(&plan), Err(PlanError::InvalidPipeline));
}

#[test]
fn rejects_non_boolean_filters_and_unscanned_references() {
    let mut plan = plan_select_v1(bound_select()).unwrap();
    plan.nodes[1] = PlanNode::Filter(BoundPredicate::Integer(1));
    assert_eq!(validate_plan_v1(&plan), Err(PlanError::InvalidPredicate));

    let mut plan = plan_select_v1(bound_select()).unwrap();
    plan.nodes[1] = PlanNode::Filter(BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(column(3, 2, ScalarType::Integer))),
        right: Box::new(BoundPredicate::Integer(1)),
    });
    assert_eq!(
        validate_plan_v1(&plan),
        Err(PlanError::InvalidColumnReference)
    );
}

#[test]
fn plan_validator_reapplies_predicate_depth_budget() {
    let mut predicate = BoundPredicate::Boolean(true);
    for _ in 0..=crate::optimizer_request::MAX_PREDICATE_DEPTH {
        predicate = BoundPredicate::Not(Box::new(predicate));
    }
    let mut plan = plan_select_v1(bound_select()).unwrap();
    plan.nodes[1] = PlanNode::Filter(predicate);
    assert_eq!(validate_plan_v1(&plan), Err(PlanError::InvalidPredicate));
}

#[test]
fn scan_contains_unique_sorted_union_of_output_and_filter_columns() {
    let mut select = bound_select();
    select.predicate = Some(BoundPredicate::Compare {
        operator: Comparison::Equal,
        left: Box::new(BoundPredicate::Column(column(1, 0, ScalarType::Integer))),
        right: Box::new(BoundPredicate::Integer(2)),
    });
    let plan = plan_select_v1(select).unwrap();
    let PlanNode::Scan { columns } = &plan.nodes[0] else {
        panic!("first node must scan")
    };
    assert_eq!(columns, &[column(1, 0, ScalarType::Integer)]);
}

#[test]
fn versioned_plan_encoding_round_trips_canonically() {
    let plan = plan_select_v1(bound_select()).unwrap();
    let bytes = encode_plan_v1(&plan).unwrap();
    assert_eq!(&bytes[..4], &[0, 1, 1, 0]);
    let decoded = decode_plan_v1(&bytes).unwrap();
    assert_eq!(decoded, plan);
    assert_eq!(encode_plan_v1(&decoded).unwrap(), bytes);
}

#[test]
fn plan_decoder_rejects_malformed_and_noncanonical_payloads() {
    let plan = plan_select_v1(bound_select()).unwrap();
    let bytes = encode_plan_v1(&plan).unwrap();

    let mut wrong_version = bytes.clone();
    wrong_version[1] = 2;
    assert_eq!(
        decode_plan_v1(&wrong_version),
        Err(PlanError::UnsupportedVersion(2))
    );

    let mut truncated = bytes.clone();
    truncated.pop();
    assert_eq!(decode_plan_v1(&truncated), Err(PlanError::Truncated));

    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(decode_plan_v1(&trailing), Err(PlanError::TrailingBytes));

    let mut unsupported_type = bytes;
    let scan_first_column_type_code = 89;
    unsupported_type[scan_first_column_type_code] = 0xff;
    assert_eq!(
        decode_plan_v1(&unsupported_type),
        Err(PlanError::InvalidEncoding)
    );

    let mut invalid_decimal = encode_plan_v1(&plan).unwrap();
    invalid_decimal[scan_first_column_type_code] = 5;
    invalid_decimal[scan_first_column_type_code + 1] = 39;
    invalid_decimal[scan_first_column_type_code + 2] = 0;
    invalid_decimal[scan_first_column_type_code + 3] = 0;
    invalid_decimal[scan_first_column_type_code + 4] = 0;
    assert_eq!(
        decode_plan_v1(&invalid_decimal),
        Err(PlanError::InvalidEncoding)
    );
}

#[test]
fn text_predicate_header_matches_the_generated_wire_record() {
    let predicate = BoundPredicate::Text("hi".to_owned());
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(encoded, [0x03, 0x00, 0x02, b'h', b'i']);

    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]).unwrap(), predicate);
    assert_eq!(reader.offset, encoded.len());
}

#[test]
fn integer_predicate_record_matches_the_generated_wire_record() {
    let predicate = BoundPredicate::Integer(-2);
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(
        encoded,
        [0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfe]
    );

    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]).unwrap(), predicate);
    assert_eq!(reader.offset, encoded.len());
}

#[test]
fn date_predicate_record_roundtrips_signed_big_endian_day_count() {
    let predicate = BoundPredicate::Date(-42);
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(encoded, [0x04, 0xff, 0xff, 0xff, 0xd6]);

    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]).unwrap(), predicate);
    assert_eq!(reader.offset, encoded.len());

    let mut invalid_date = encoded;
    invalid_date[1..].copy_from_slice(&i32::MAX.to_be_bytes());
    let mut reader = PlanReader {
        bytes: &invalid_date,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]), Err(PlanError::InvalidPredicate));
}

#[test]
fn uuid_predicate_record_roundtrips_canonical_bytes() {
    let predicate = BoundPredicate::Uuid([0x12; 16]);
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(encoded, [&[0x05][..], &[0x12; 16][..]].concat());

    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]).unwrap(), predicate);
    assert_eq!(reader.offset, encoded.len());
}

#[test]
fn timestamptz_predicate_record_roundtrips_signed_microseconds() {
    let predicate = BoundPredicate::TimestampTz(-42);
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(encoded, [&[0x06][..], &(-42i64).to_be_bytes()[..]].concat());
    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]).unwrap(), predicate);
    let mut invalid = encoded;
    invalid[1..].copy_from_slice(&i64::MAX.to_be_bytes());
    let mut reader = PlanReader {
        bytes: &invalid,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]), Err(PlanError::InvalidPredicate));
}

#[test]
fn bytea_predicate_record_roundtrips_bounded_raw_bytes() {
    let predicate = BoundPredicate::Bytea(vec![0, 0x80, 0xff]);
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(encoded, [0x07, 0, 3, 0, 0x80, 0xff]);
    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]).unwrap(), predicate);
    let mut output = Vec::new();
    assert_eq!(
        put_predicate(
            &mut output,
            &BoundPredicate::Bytea(vec![
                0;
                crate::optimizer_request::MAX_TEXT_LITERAL_BYTES + 1
            ]),
        ),
        Err(PlanError::InvalidPredicate)
    );
}

#[test]
fn decimal_predicate_record_roundtrips_signed_big_endian_coefficient() {
    let predicate = BoundPredicate::Decimal(-123_456);
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(
        encoded,
        [&[0x08][..], &(-123_456i128).to_be_bytes()[..]].concat()
    );
    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]).unwrap(), predicate);
}

#[test]
fn decimal_predicate_requires_a_decimal_column_and_fits_its_precision() {
    let decimal_column = column(
        0x46,
        0,
        ScalarType::Decimal {
            precision: 8,
            scale: 2,
        },
    );
    let predicate = BoundPredicate::Compare {
        operator: crate::optimizer_request::Comparison::Equal,
        left: Box::new(BoundPredicate::Column(decimal_column.clone())),
        right: Box::new(BoundPredicate::Decimal(12_345_678)),
    };
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(
        decode_bound_predicate_v1(&encoded, std::slice::from_ref(&decimal_column)),
        Ok(predicate)
    );

    let out_of_precision = BoundPredicate::Compare {
        operator: crate::optimizer_request::Comparison::Equal,
        left: Box::new(BoundPredicate::Column(decimal_column)),
        right: Box::new(BoundPredicate::Decimal(123_456_789)),
    };
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &out_of_precision).unwrap();
    let columns = [column(
        0x46,
        0,
        ScalarType::Decimal {
            precision: 8,
            scale: 2,
        },
    )];
    let decoded = decode_bound_predicate_v1(&encoded, &columns).unwrap();
    assert_eq!(
        predicate_type(&decoded, &columns),
        Err(PlanError::InvalidPredicate)
    );
}

#[test]
fn uuid_literals_are_eligible_for_exact_index_access() {
    let key = column(0x45, 0, ScalarType::Uuid);
    assert!(is_point_probe_literal(
        &key,
        &BoundPredicate::Uuid([0x12; 16])
    ));
}

#[test]
fn timestamptz_literals_are_eligible_for_exact_index_access() {
    let key = column(0x45, 0, ScalarType::TimestampTz);
    assert!(is_point_probe_literal(
        &key,
        &BoundPredicate::TimestampTz(-42)
    ));
}

#[test]
fn bytea_literals_are_eligible_for_exact_index_access() {
    let key = column(0x45, 0, ScalarType::Bytea);
    assert!(is_point_probe_literal(
        &key,
        &BoundPredicate::Bytea(vec![0, 0x80, 0xff])
    ));
}

#[test]
fn decimal_literals_are_eligible_for_exact_index_access() {
    let key = column(
        0x45,
        0,
        ScalarType::Decimal {
            precision: 8,
            scale: 2,
        },
    );
    assert!(is_point_probe_literal(
        &key,
        &BoundPredicate::Decimal(123_456)
    ));
    assert!(!is_point_probe_literal(
        &key,
        &BoundPredicate::Decimal(123_456_789)
    ));
}

#[test]
fn column_predicate_record_matches_the_generated_wire_record() {
    let source_column = column(0xab, 0, ScalarType::Integer);
    let predicate = BoundPredicate::Column(source_column.clone());
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(encoded, [&[0x10][..], &[0xabu8; 16][..]].concat());

    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[source_column]).unwrap(), predicate);
    assert_eq!(reader.offset, encoded.len());
}

#[test]
fn compare_predicate_header_matches_the_generated_wire_record() {
    let predicate = BoundPredicate::Compare {
        operator: Comparison::GreaterOrEqual,
        left: Box::new(BoundPredicate::Null),
        right: Box::new(BoundPredicate::Null),
    };
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(encoded, [0x20, 0x05, 0x00, 0x00]);

    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]).unwrap(), predicate);
    assert_eq!(reader.offset, encoded.len());
}

#[test]
fn boolean_predicate_header_matches_the_generated_wire_record() {
    let predicate = BoundPredicate::And(vec![
        BoundPredicate::Boolean(true),
        BoundPredicate::Boolean(false),
    ]);
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(encoded, [0x30, 0x02, 0x01, 0x01, 0x01, 0x00]);

    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]).unwrap(), predicate);
    assert_eq!(reader.offset, encoded.len());
}

#[test]
fn name_header_matches_the_generated_wire_record() {
    let mut encoded = Vec::new();
    put_name(&mut encoded, "sales").unwrap();
    assert_eq!(encoded, [0x05, b's', b'a', b'l', b'e', b's']);

    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.name().unwrap(), "sales");
    assert_eq!(reader.offset, encoded.len());
}

#[test]
fn not_predicate_record_matches_the_generated_wire_record() {
    let predicate = BoundPredicate::Not(Box::new(BoundPredicate::Boolean(true)));
    let mut encoded = Vec::new();
    put_predicate(&mut encoded, &predicate).unwrap();
    assert_eq!(encoded, [0x32, 0x01, 0x01]);

    let mut reader = PlanReader {
        bytes: &encoded,
        offset: 0,
    };
    assert_eq!(reader.predicate(&[]).unwrap(), predicate);
    assert_eq!(reader.offset, encoded.len());
}

#[test]
fn null_and_boolean_predicate_records_match_the_generated_wire_records() {
    for (predicate, expected) in [
        (BoundPredicate::Null, vec![0x00]),
        (BoundPredicate::Boolean(true), vec![0x01, 0x01]),
        (BoundPredicate::Boolean(false), vec![0x01, 0x00]),
    ] {
        let mut encoded = Vec::new();
        put_predicate(&mut encoded, &predicate).unwrap();
        assert_eq!(encoded, expected);

        let mut reader = PlanReader {
            bytes: &encoded,
            offset: 0,
        };
        assert_eq!(reader.predicate(&[]).unwrap(), predicate);
        assert_eq!(reader.offset, encoded.len());
    }
}

#[test]
fn plan_encoder_rejects_invalid_scalar_type_parameters() {
    let mut plan = plan_select_v1(bound_select()).unwrap();
    let PlanNode::Scan { columns } = &mut plan.nodes[0] else {
        panic!("first node must scan")
    };
    columns[0].scalar_type = ScalarType::Decimal {
        precision: 0,
        scale: 0,
    };
    assert_eq!(encode_plan_v1(&plan), Err(PlanError::InvalidScalarType));
}

#[test]
fn group_count_plan_roundtrips_and_rejects_unscanned_group_key() {
    let key = column(4, 1, ScalarType::Text);
    let plan = plan_group_count_v1(BoundGroupCount {
        schema_name: "sales".to_owned(),
        schema_id: [0x10; 16],
        schema_version: 4,
        table_epoch: 6,
        relation_id: [0x20; 16],
        relation_name: "orders".to_owned(),
        group_key: key.clone(),
        limit: Some(7),
    })
    .unwrap();
    let bytes = encode_group_count_plan_v1(&plan).unwrap();
    assert_eq!(decode_group_count_plan_v1(&bytes).unwrap(), plan);

    let mut invalid = plan;
    invalid.scan_columns.clear();
    assert_eq!(
        validate_group_count_plan_v1(&invalid),
        Err(PlanError::InvalidColumnReference)
    );
}

#[test]
fn group_aggregate_plan_roundtrips_and_checks_argument_scan_shape() {
    let plan = plan_group_aggregate_v1(BoundGroupAggregate {
        schema_name: "public".into(),
        schema_id: [1; 16],
        schema_version: 1,
        table_epoch: 1,
        relation_id: [2; 16],
        relation_name: "items".into(),
        group_key: column(1, 0, ScalarType::Integer),
        aggregate_kind: 4,
        aggregate_column: Some(column(2, 1, ScalarType::BigInt)),
        limit: Some(5),
    })
    .unwrap();
    let bytes = encode_group_aggregate_plan_v1(&plan).unwrap();
    assert_eq!(decode_group_aggregate_plan_v1(&bytes).unwrap(), plan);
    let mut invalid = plan;
    invalid.scan_columns.pop();
    assert_eq!(
        validate_group_aggregate_plan_v1(&invalid),
        Err(PlanError::InvalidColumnReference)
    );

    let count_column_plan = plan_group_aggregate_v1(BoundGroupAggregate {
        schema_name: "public".into(),
        schema_id: [1; 16],
        schema_version: 1,
        table_epoch: 1,
        relation_id: [2; 16],
        relation_name: "items".into(),
        group_key: column(1, 0, ScalarType::Integer),
        aggregate_kind: 6,
        aggregate_column: Some(column(3, 2, ScalarType::Boolean)),
        limit: None,
    })
    .unwrap();
    let bytes = encode_group_aggregate_plan_v1(&count_column_plan).unwrap();
    assert_eq!(
        decode_group_aggregate_plan_v1(&bytes).unwrap(),
        count_column_plan
    );
}

#[test]
fn ordered_group_aggregate_plan_roundtrips_and_rejects_bad_order_sources() {
    let mut avg_plan = None;
    for aggregate_kind in 1..=6 {
        let plan = plan_ordered_group_aggregate_v1(BoundOrderedGroupAggregate {
            schema_name: "public".into(),
            schema_id: [1; 16],
            schema_version: 1,
            table_epoch: 1,
            relation_id: [2; 16],
            relation_name: "items".into(),
            group_key: column(1, 0, ScalarType::Integer),
            aggregate_kind,
            aggregate_column: (aggregate_kind != 1).then(|| column(2, 1, ScalarType::BigInt)),
            order_items: vec![
                GroupCountOrderItem {
                    source: 1,
                    descending: true,
                    nulls_first: false,
                },
                GroupCountOrderItem {
                    source: 0,
                    descending: false,
                    nulls_first: true,
                },
            ],
            limit: Some(5),
        })
        .unwrap();
        let bytes = encode_ordered_group_aggregate_plan_v1(&plan).unwrap();
        assert_eq!(&bytes[..4], &[0, 1, 11, 0]);
        assert_eq!(
            decode_ordered_group_aggregate_plan_v1(&bytes).unwrap(),
            plan
        );
        if aggregate_kind == 5 {
            avg_plan = Some(plan);
        }
    }

    let mut invalid = avg_plan.unwrap();
    invalid.order_items[1].source = 1;
    assert_eq!(
        validate_ordered_group_aggregate_plan_v1(&invalid),
        Err(PlanError::InvalidColumnReference)
    );
}

#[test]
fn ordered_group_count_plan_roundtrips_and_rejects_duplicate_sort_sources() {
    let key = column(4, 1, ScalarType::Text);
    let plan = plan_ordered_group_count_v1(BoundOrderedGroupCount {
        schema_name: "sales".to_owned(),
        schema_id: [0x10; 16],
        schema_version: 4,
        table_epoch: 6,
        relation_id: [0x20; 16],
        relation_name: "orders".to_owned(),
        group_key: key,
        order_items: vec![
            GroupCountOrderItem {
                source: 1,
                descending: true,
                nulls_first: false,
            },
            GroupCountOrderItem {
                source: 0,
                descending: false,
                nulls_first: true,
            },
        ],
        limit: Some(7),
    })
    .unwrap();
    let bytes = encode_ordered_group_count_plan_v1(&plan).unwrap();
    assert_eq!(&bytes[..4], &[0, 1, 8, 0]);
    assert_eq!(decode_ordered_group_count_plan_v1(&bytes).unwrap(), plan);

    let mut invalid = plan;
    invalid.order_items[1].source = 1;
    assert_eq!(
        validate_ordered_group_count_plan_v1(&invalid),
        Err(PlanError::InvalidColumnReference)
    );
}

#[test]
fn ordered_composite_group_count_plan_roundtrips_and_validates_order_sources() {
    use crate::optimizer_request::CompositeGroupCountOrderItem;
    let keys = vec![
        column(2, 1, ScalarType::Boolean),
        column(1, 0, ScalarType::Integer),
    ];
    let plan = plan_ordered_composite_group_count_v1(BoundOrderedCompositeGroupCount {
        schema_name: "public".into(),
        schema_id: [1; 16],
        schema_version: 1,
        table_epoch: 1,
        relation_id: [2; 16],
        relation_name: "items".into(),
        group_keys: keys,
        order_items: vec![
            CompositeGroupCountOrderItem {
                source: 1,
                group_key_index: None,
                descending: true,
                nulls_first: false,
            },
            CompositeGroupCountOrderItem {
                source: 0,
                group_key_index: Some(0),
                descending: false,
                nulls_first: true,
            },
        ],
        limit: Some(4),
    })
    .unwrap();
    assert_eq!(
        plan.scan_columns
            .iter()
            .map(|column| column.ordinal)
            .collect::<Vec<_>>(),
        [0, 1]
    );
    let bytes = encode_ordered_composite_group_count_plan_v1(&plan).unwrap();
    assert_eq!(
        decode_ordered_composite_group_count_plan_v1(&bytes).unwrap(),
        plan
    );
    let mut invalid = plan;
    invalid.order_items[1].group_key_index = Some(1);
    invalid.order_items.push(CompositeGroupCountOrderItem {
        source: 0,
        group_key_index: Some(1),
        descending: false,
        nulls_first: false,
    });
    assert_eq!(
        validate_ordered_composite_group_count_plan_v1(&invalid),
        Err(PlanError::InvalidColumnReference)
    );
}

#[test]
fn composite_group_count_plan_preserves_key_order_and_canonical_scan_shape() {
    let first = column(4, 1, ScalarType::Text);
    let second = column(3, 0, ScalarType::Integer);
    let plan = plan_composite_group_count_v1(BoundCompositeGroupCount {
        schema_name: "sales".to_owned(),
        schema_id: [0x10; 16],
        schema_version: 4,
        table_epoch: 6,
        relation_id: [0x20; 16],
        relation_name: "orders".to_owned(),
        group_keys: vec![first.clone(), second.clone()],
        limit: Some(7),
    })
    .unwrap();
    assert_eq!(plan.group_keys, [first.clone(), second.clone()]);
    assert_eq!(plan.scan_columns, [second, first]);
    let bytes = encode_composite_group_count_plan_v1(&plan).unwrap();
    assert_eq!(&bytes[..4], &[0, 1, 6, 0]);
    assert_eq!(decode_composite_group_count_plan_v1(&bytes).unwrap(), plan);

    let mut invalid = plan;
    invalid.group_keys[1] = invalid.group_keys[0].clone();
    assert_eq!(
        validate_composite_group_count_plan_v1(&invalid),
        Err(PlanError::InvalidColumnReference)
    );
}

#[test]
fn composite_group_aggregate_plan_round_trips_and_checks_scan_union() {
    let first = column(4, 1, ScalarType::Text);
    let second = column(3, 0, ScalarType::Integer);
    let amount = column(5, 2, ScalarType::BigInt);
    let plan = plan_composite_group_aggregate_v1(BoundCompositeGroupAggregate {
        schema_name: "sales".to_owned(),
        schema_id: [0x10; 16],
        schema_version: 4,
        table_epoch: 6,
        relation_id: [0x20; 16],
        relation_name: "orders".to_owned(),
        group_keys: vec![first.clone(), second.clone()],
        aggregate_kind: 4,
        aggregate_column: Some(amount.clone()),
        limit: Some(7),
    })
    .unwrap();
    assert_eq!(plan.group_keys, [first.clone(), second.clone()]);
    assert_eq!(plan.scan_columns, [second, first, amount]);
    let bytes = encode_composite_group_aggregate_plan_v1(&plan).unwrap();
    assert_eq!(&bytes[..4], &[0, 1, 13, 0]);
    assert_eq!(
        decode_composite_group_aggregate_plan_v1(&bytes).unwrap(),
        plan
    );

    let mut invalid = plan;
    invalid.scan_columns.pop();
    assert_eq!(
        validate_composite_group_aggregate_plan_v1(&invalid),
        Err(PlanError::InvalidColumnReference)
    );
}

#[test]
fn filtered_composite_group_aggregate_plan_round_trips_filter_only_columns() {
    use crate::optimizer_binding::{BoundColumn, BoundPredicate};
    use crate::optimizer_request::{Comparison, CompositeGroupCountOrderItem};

    let first = column(4, 1, ScalarType::Text);
    let second = column(3, 0, ScalarType::Integer);
    let amount = column(5, 2, ScalarType::BigInt);
    let predicate = BoundPredicate::And(vec![
        BoundPredicate::IsNotNull(Box::new(BoundPredicate::Column(amount.clone()))),
        BoundPredicate::Compare {
            operator: Comparison::Greater,
            left: Box::new(BoundPredicate::Column(BoundColumn {
                column_id: [0x26; 16],
                ordinal: 3,
                scalar_type: ScalarType::Integer,
                nullable: false,
            })),
            right: Box::new(BoundPredicate::Integer(7)),
        },
    ]);
    let plan = plan_filtered_composite_group_aggregate_v1(BoundFilteredCompositeGroupAggregate {
        schema_name: "sales".to_owned(),
        schema_id: [0x10; 16],
        schema_version: 4,
        table_epoch: 6,
        relation_id: [0x20; 16],
        relation_name: "orders".to_owned(),
        group_keys: vec![first.clone(), second.clone()],
        aggregate_kind: 4,
        aggregate_column: Some(amount.clone()),
        predicate,
        order_items: Vec::new(),
        limit: Some(7),
    })
    .unwrap();
    assert_eq!(plan.scan_columns.len(), 4);
    assert_eq!(plan.scan_columns[3].ordinal, 3);
    let bytes = encode_filtered_composite_group_aggregate_plan_v1(&plan).unwrap();
    assert_eq!(&bytes[..4], &[0, 1, 15, 0]);
    assert_eq!(
        decode_filtered_composite_group_aggregate_plan_v1(&bytes).unwrap(),
        plan
    );

    let mut ordered_plan = plan.clone();
    ordered_plan.order_items = vec![
        CompositeGroupCountOrderItem {
            source: 0,
            group_key_index: Some(0),
            descending: true,
            nulls_first: false,
        },
        CompositeGroupCountOrderItem {
            source: 1,
            group_key_index: None,
            descending: false,
            nulls_first: true,
        },
    ];
    let ordered_bytes = encode_filtered_composite_group_aggregate_plan_v1(&ordered_plan).unwrap();
    assert_eq!(&ordered_bytes[..4], &[0, 1, 16, 0]);
    assert_eq!(
        decode_filtered_composite_group_aggregate_plan_v1(&ordered_bytes).unwrap(),
        ordered_plan
    );

    let single_key_plan =
        plan_filtered_composite_group_aggregate_v1(BoundFilteredCompositeGroupAggregate {
            schema_name: "sales".to_owned(),
            schema_id: [0x10; 16],
            schema_version: 4,
            table_epoch: 6,
            relation_id: [0x20; 16],
            relation_name: "orders".to_owned(),
            group_keys: vec![second],
            aggregate_kind: 4,
            aggregate_column: Some(amount),
            predicate: BoundPredicate::IsNotNull(Box::new(BoundPredicate::Column(first.clone()))),
            order_items: Vec::new(),
            limit: Some(7),
        })
        .unwrap();
    assert_eq!(single_key_plan.group_keys.len(), 1);
    assert_eq!(single_key_plan.scan_columns.len(), 3);
    let single_key_bytes =
        encode_filtered_composite_group_aggregate_plan_v1(&single_key_plan).unwrap();
    assert_eq!(
        decode_filtered_composite_group_aggregate_plan_v1(&single_key_bytes).unwrap(),
        single_key_plan
    );

    let mut invalid = plan;
    invalid.scan_columns.pop();
    assert_eq!(
        validate_filtered_composite_group_aggregate_plan_v1(&invalid),
        Err(PlanError::InvalidColumnReference)
    );
}

#[test]
fn ordered_composite_group_aggregate_plan_roundtrips_and_checks_sort_items() {
    let keys = vec![
        column(4, 1, ScalarType::Text),
        column(3, 0, ScalarType::Integer),
    ];
    let amount = column(5, 2, ScalarType::BigInt);
    let plan = plan_ordered_composite_group_aggregate_v1(BoundOrderedCompositeGroupAggregate {
        schema_name: "sales".into(),
        schema_id: [0x10; 16],
        schema_version: 4,
        table_epoch: 6,
        relation_id: [0x20; 16],
        relation_name: "orders".into(),
        group_keys: keys,
        aggregate_kind: 4,
        aggregate_column: Some(amount),
        order_items: vec![
            CompositeGroupCountOrderItem {
                source: 1,
                group_key_index: None,
                descending: true,
                nulls_first: false,
            },
            CompositeGroupCountOrderItem {
                source: 0,
                group_key_index: Some(1),
                descending: false,
                nulls_first: true,
            },
        ],
        limit: Some(4),
    })
    .unwrap();
    let bytes = encode_ordered_composite_group_aggregate_plan_v1(&plan).unwrap();
    assert_eq!(&bytes[..4], &[0, 1, 14, 0]);
    assert_eq!(
        decode_ordered_composite_group_aggregate_plan_v1(&bytes).unwrap(),
        plan
    );

    let mut invalid = plan;
    invalid.order_items[1].group_key_index = Some(0);
    invalid.order_items.push(CompositeGroupCountOrderItem {
        source: 0,
        group_key_index: Some(0),
        descending: false,
        nulls_first: false,
    });
    assert_eq!(
        validate_ordered_composite_group_aggregate_plan_v1(&invalid),
        Err(PlanError::InvalidColumnReference)
    );
}

#[test]
fn join_plan_round_trips_and_rejects_unscanned_keys() {
    let left_key = column(1, 0, ScalarType::Integer);
    let left_payload = column(2, 1, ScalarType::Text);
    let left_key2 = column(3, 2, ScalarType::BigInt);
    let right_key = column(4, 0, ScalarType::Integer);
    let right_payload = column(5, 1, ScalarType::Text);
    let right_key2 = column(6, 2, ScalarType::BigInt);
    let plan = JoinSelectPlanV1 {
        schema_name: "sales".to_owned(),
        schema_id: [0x10; 16],
        schema_version: 7,
        left_relation: RelationReference {
            schema_name: "sales".to_owned(),
            schema_id: [0x10; 16],
            schema_version: 7,
            relation_name: "orders".to_owned(),
            relation_id: [0x20; 16],
            table_epoch: 11,
        },
        right_relation: RelationReference {
            schema_name: "sales".to_owned(),
            schema_id: [0x10; 16],
            schema_version: 7,
            relation_name: "customers".to_owned(),
            relation_id: [0x30; 16],
            table_epoch: 12,
        },
        left_scan_columns: vec![left_key.clone(), left_payload.clone(), left_key2.clone()],
        right_scan_columns: vec![right_key.clone(), right_payload.clone(), right_key2.clone()],
        projections: vec![
            (JoinRelationSide::Left, left_payload),
            (JoinRelationSide::Right, right_payload),
        ],
        join_keys: vec![(left_key, right_key), (left_key2, right_key2)],
        limit: Some(9),
        filter: None,
        index_nested_loop: None,
    };
    let bytes = encode_join_select_plan_v1(&plan).unwrap();
    assert_eq!(decode_join_select_plan_v1(&bytes).unwrap(), plan);

    let mut filtered = plan.clone();
    filtered.filter = Some(BoundJoinFilter {
        relation_side: JoinRelationSide::Left,
        column: filtered.left_scan_columns[1].clone(),
        expression: crate::optimizer_request::JoinFilterExpression::Parameter {
            operator: crate::optimizer_request::Comparison::Greater,
            index: 3,
        },
    });
    let filtered_bytes = encode_join_select_plan_v1(&filtered).unwrap();
    assert_eq!(
        crate::query_abi::decode_optimizer_plan_header_record(&filtered_bytes)
            .unwrap()
            .1,
        PLAN_KIND_FILTERED_JOIN_SELECT
    );
    assert_eq!(
        decode_join_select_plan_v1(&filtered_bytes).unwrap(),
        filtered
    );
    let mut invalid_filter = filtered.clone();
    invalid_filter.filter.as_mut().unwrap().column.ordinal = 99;
    assert_eq!(
        validate_join_select_plan_v1(&invalid_filter),
        Err(PlanError::InvalidColumnReference)
    );

    let mut invalid = plan;
    invalid.join_keys[0].0.column_id = [0x7f; 16];
    assert_eq!(
        validate_join_select_plan_v1(&invalid),
        Err(PlanError::InvalidColumnReference)
    );
}
