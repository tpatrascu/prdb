//! Authorized-view cost-based physical planner.

use super::rewrite::{
    collect_expr_column_ids, combine_conjuncts, fold_constant_bool, split_conjuncts,
};
use super::{CostWeights, PlanCost, StatsCatalog};
use crate::optimizer_ir::bind::{
    BoundLogicalOp, BoundLogicalRequest, BoundOrderItem, BoundOrderTarget, BoundRelation,
};
use crate::optimizer_ir::expr::{BoundColumn, BoundExpr, Comparison};
use crate::optimizer_ir::logical::JoinType;
use crate::optimizer_ir::physical::{
    BoundAggregateSpec, IndexBound, PhysicalOp, PhysicalOrderItem, PhysicalPlan,
    PhysicalUpdateAssignment, PlanError, RelationReference,
};
use crate::statistics::AuthorizedRelationStats;
use std::collections::HashSet;

/// Optimize a bound logical request into a cost-optimal `PhysicalPlan` using
/// only authorized-view statistics (`StatsCatalog`) and public `CostWeights`.
pub fn optimize_bound_request(
    req: &BoundLogicalRequest,
    stats: &StatsCatalog,
    weights: &CostWeights,
) -> Result<PhysicalPlan, PlanError> {
    let mut required = HashSet::new();
    collect_root_required_columns(&req.root, &mut required);
    let (root, _) = plan_op(&req.root, &required, None, None, stats, weights)?;
    let plan = PhysicalPlan {
        schema_name: req.schema_name.clone(),
        schema_id: req.schema_id,
        schema_version: req.schema_version,
        root,
    };
    crate::optimizer_ir::validate_physical_plan(&plan)?;
    Ok(plan)
}

fn collect_root_required_columns(op: &BoundLogicalOp, out: &mut HashSet<(u16, [u8; 16])>) {
    match op {
        BoundLogicalOp::Scan { relation } => {
            for c in &relation.columns {
                out.insert((relation.relation_index, c.column_id));
            }
        }
        BoundLogicalOp::Filter { input, predicate } => {
            collect_expr_column_ids(predicate, out);
            collect_root_required_columns(input, out);
        }
        BoundLogicalOp::Project { input, columns } => {
            for c in columns {
                out.insert((c.relation_index, c.column.column_id));
            }
            // Only collect non-Scan requirements below Project so column pruning works.
            collect_non_scan_expr_columns(input, out);
        }
        BoundLogicalOp::Join {
            left,
            right,
            eq_keys,
            residual,
            ..
        } => {
            for (l, r) in eq_keys {
                out.insert((l.relation_index, l.column.column_id));
                out.insert((r.relation_index, r.column.column_id));
            }
            if let Some(res) = residual {
                collect_expr_column_ids(res, out);
            }
            collect_root_required_columns(left, out);
            collect_root_required_columns(right, out);
        }
        BoundLogicalOp::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
        } => {
            for k in group_keys {
                out.insert((k.relation_index, k.column.column_id));
            }
            for a in aggregates {
                if let Some(arg) = &a.arg {
                    out.insert((arg.relation_index, arg.column.column_id));
                }
            }
            if let Some(h) = having {
                collect_expr_column_ids(h, out);
            }
            collect_non_scan_expr_columns(input, out);
        }
        BoundLogicalOp::Sort { input, order_by } => {
            for item in order_by {
                if let BoundOrderTarget::Column(c) = &item.target {
                    out.insert((c.relation_index, c.column.column_id));
                }
            }
            collect_root_required_columns(input, out);
        }
        BoundLogicalOp::Limit { input, .. } => collect_root_required_columns(input, out),
        BoundLogicalOp::Delete {
            relation,
            predicate,
        } => {
            collect_expr_column_ids(predicate, out);
            if out.is_empty()
                && let Some(first) = relation.columns.first()
            {
                out.insert((relation.relation_index, first.column_id));
            }
        }
        BoundLogicalOp::Update {
            relation,
            assignments,
            predicate,
        } => {
            collect_expr_column_ids(predicate, out);
            for a in assignments {
                out.insert((relation.relation_index, a.column.column_id));
            }
        }
    }
}

fn collect_non_scan_expr_columns(op: &BoundLogicalOp, out: &mut HashSet<(u16, [u8; 16])>) {
    match op {
        BoundLogicalOp::Scan { .. } => {}
        BoundLogicalOp::Filter { input, predicate } => {
            collect_expr_column_ids(predicate, out);
            collect_non_scan_expr_columns(input, out);
        }
        BoundLogicalOp::Project { input, columns } => {
            for c in columns {
                out.insert((c.relation_index, c.column.column_id));
            }
            collect_non_scan_expr_columns(input, out);
        }
        BoundLogicalOp::Join {
            left,
            right,
            eq_keys,
            residual,
            ..
        } => {
            for (l, r) in eq_keys {
                out.insert((l.relation_index, l.column.column_id));
                out.insert((r.relation_index, r.column.column_id));
            }
            if let Some(res) = residual {
                collect_expr_column_ids(res, out);
            }
            collect_non_scan_expr_columns(left, out);
            collect_non_scan_expr_columns(right, out);
        }
        BoundLogicalOp::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
        } => {
            for k in group_keys {
                out.insert((k.relation_index, k.column.column_id));
            }
            for a in aggregates {
                if let Some(arg) = &a.arg {
                    out.insert((arg.relation_index, arg.column.column_id));
                }
            }
            if let Some(h) = having {
                collect_expr_column_ids(h, out);
            }
            collect_non_scan_expr_columns(input, out);
        }
        BoundLogicalOp::Sort { input, order_by } => {
            for item in order_by {
                if let BoundOrderTarget::Column(c) = &item.target {
                    out.insert((c.relation_index, c.column.column_id));
                }
            }
            collect_non_scan_expr_columns(input, out);
        }
        BoundLogicalOp::Limit { input, .. } => collect_non_scan_expr_columns(input, out),
        BoundLogicalOp::Delete { predicate, .. } => collect_expr_column_ids(predicate, out),
        BoundLogicalOp::Update {
            relation,
            assignments,
            predicate,
        } => {
            collect_expr_column_ids(predicate, out);
            for a in assignments {
                out.insert((relation.relation_index, a.column.column_id));
            }
        }
    }
}

fn plan_op(
    op: &BoundLogicalOp,
    required: &HashSet<(u16, [u8; 16])>,
    desired_order: Option<&[BoundOrderItem]>,
    limit_hint: Option<u32>,
    stats: &StatsCatalog,
    weights: &CostWeights,
) -> Result<(PhysicalOp, PlanCost), PlanError> {
    match op {
        BoundLogicalOp::Limit { input, count } => {
            if *count == 0 {
                let cols = output_columns_for_op(input, required);
                return Ok((
                    PhysicalOp::EmptyResult {
                        output_columns: cols,
                    },
                    PlanCost {
                        total: 0.0,
                        estimated_rows: 0.0,
                        estimated_row_bytes: 0.0,
                        ipc_roundtrips: 0.0,
                        spills: false,
                    },
                ));
            }
            // If child is Sort, try TopNHeap or IndexOrderedScan pushdown.
            if let BoundLogicalOp::Sort {
                input: sort_input,
                order_by,
            } = input.as_ref()
            {
                let (child_op, child_cost) = plan_op(
                    sort_input,
                    required,
                    Some(order_by),
                    Some(*count),
                    stats,
                    weights,
                )?;
                if is_ordered_by(&child_op, order_by) {
                    let est_rows = child_cost.estimated_rows.min(f64::from(*count));
                    return Ok((
                        PhysicalOp::Limit {
                            input: Box::new(child_op),
                            count: *count,
                        },
                        PlanCost {
                            estimated_rows: est_rows,
                            ..child_cost
                        },
                    ));
                }
                let p_items = to_physical_order(order_by);
                let topn_cost = PlanCost {
                    total: child_cost.total + child_cost.estimated_rows * weights.cpu_row_weight,
                    estimated_rows: child_cost.estimated_rows.min(f64::from(*count)),
                    estimated_row_bytes: child_cost.estimated_row_bytes,
                    ipc_roundtrips: child_cost.ipc_roundtrips,
                    spills: false,
                };
                return Ok((
                    PhysicalOp::TopNHeap {
                        input: Box::new(child_op),
                        order_by: p_items,
                        limit: *count,
                    },
                    topn_cost,
                ));
            }
            let (child_op, child_cost) =
                plan_op(input, required, desired_order, Some(*count), stats, weights)?;
            if matches!(child_op, PhysicalOp::EmptyResult { .. }) {
                return Ok((child_op, child_cost));
            }
            Ok((
                PhysicalOp::Limit {
                    input: Box::new(child_op),
                    count: *count,
                },
                PlanCost {
                    estimated_rows: child_cost.estimated_rows.min(f64::from(*count)),
                    ..child_cost
                },
            ))
        }
        BoundLogicalOp::Sort { input, order_by } => {
            let (child_op, child_cost) =
                plan_op(input, required, Some(order_by), limit_hint, stats, weights)?;
            if matches!(child_op, PhysicalOp::EmptyResult { .. })
                || is_ordered_by(&child_op, order_by)
            {
                return Ok((child_op, child_cost));
            }
            let total_bytes = child_cost.estimated_rows * child_cost.estimated_row_bytes;
            let allow_spill = total_bytes > weights.in_memory_budget_bytes as f64;
            let sort_cpu = child_cost.estimated_rows
                * approx_log2(child_cost.estimated_rows.max(2.0))
                * weights.cpu_row_weight;
            let spill_cost = if allow_spill {
                total_bytes * weights.spill_byte_weight
            } else {
                0.0
            };
            Ok((
                PhysicalOp::Sort {
                    input: Box::new(child_op),
                    order_by: to_physical_order(order_by),
                    allow_spill,
                },
                PlanCost {
                    total: child_cost.total + sort_cpu + spill_cost,
                    spills: child_cost.spills || allow_spill,
                    ..child_cost
                },
            ))
        }
        BoundLogicalOp::Project { input, columns } => {
            let (child_op, child_cost) =
                plan_op(input, required, desired_order, limit_hint, stats, weights)?;
            if matches!(child_op, PhysicalOp::EmptyResult { .. }) {
                return Ok((
                    PhysicalOp::EmptyResult {
                        output_columns: columns.iter().map(|c| c.column.clone()).collect(),
                    },
                    child_cost,
                ));
            }
            Ok((
                PhysicalOp::Project {
                    input: Box::new(child_op),
                    columns: columns.iter().map(|c| c.column.clone()).collect(),
                },
                child_cost,
            ))
        }
        BoundLogicalOp::Filter { input, predicate } => {
            if let Some(false) = fold_constant_bool(predicate) {
                let cols = output_columns_for_op(input, required);
                return Ok((
                    PhysicalOp::EmptyResult {
                        output_columns: cols,
                    },
                    PlanCost {
                        total: 0.0,
                        estimated_rows: 0.0,
                        estimated_row_bytes: 0.0,
                        ipc_roundtrips: 0.0,
                        spills: false,
                    },
                ));
            }
            if let BoundLogicalOp::Scan { relation } = input.as_ref() {
                return plan_filtered_scan(
                    relation,
                    Some(predicate),
                    required,
                    desired_order,
                    limit_hint,
                    stats,
                    weights,
                );
            }
            let (child_op, child_cost) =
                plan_op(input, required, desired_order, limit_hint, stats, weights)?;
            if let Some(true) = fold_constant_bool(predicate) {
                return Ok((child_op, child_cost));
            }
            let sel = estimate_expr_selectivity(predicate, stats);
            Ok((
                PhysicalOp::Filter {
                    input: Box::new(child_op),
                    predicate: predicate.clone(),
                },
                PlanCost {
                    total: child_cost.total + child_cost.estimated_rows * weights.cpu_row_weight,
                    estimated_rows: (child_cost.estimated_rows * sel).max(1.0),
                    ..child_cost
                },
            ))
        }
        BoundLogicalOp::Scan { relation } => plan_filtered_scan(
            relation,
            None,
            required,
            desired_order,
            limit_hint,
            stats,
            weights,
        ),
        BoundLogicalOp::Join {
            join_type,
            left,
            right,
            eq_keys,
            residual,
        } => {
            let (left_op, left_cost) = plan_op(left, required, None, None, stats, weights)?;
            let (right_op, right_cost) = plan_op(right, required, None, None, stats, weights)?;
            if matches!(left_op, PhysicalOp::EmptyResult { .. }) {
                return Ok((left_op, left_cost));
            }
            if matches!(right_op, PhysicalOp::EmptyResult { .. })
                && matches!(join_type, JoinType::Inner | JoinType::Semi)
            {
                let cols = output_columns_for_op(op, required);
                return Ok((
                    PhysicalOp::EmptyResult {
                        output_columns: cols,
                    },
                    PlanCost {
                        total: 0.0,
                        estimated_rows: 0.0,
                        estimated_row_bytes: 0.0,
                        ipc_roundtrips: 0.0,
                        spills: false,
                    },
                ));
            }

            let left_keys: Vec<BoundColumn> =
                eq_keys.iter().map(|(l, _)| l.column.clone()).collect();
            let right_keys: Vec<BoundColumn> =
                eq_keys.iter().map(|(_, r)| r.column.clone()).collect();

            // Cost HashJoin (with adaptive spill when build side exceeds budget).
            let build_bytes = left_cost.estimated_rows.min(right_cost.estimated_rows)
                * left_cost
                    .estimated_row_bytes
                    .max(right_cost.estimated_row_bytes);
            let hash_spills = build_bytes > weights.in_memory_budget_bytes as f64;
            let hash_total = left_cost.total
                + right_cost.total
                + (left_cost.estimated_rows + right_cost.estimated_rows) * weights.cpu_row_weight
                + if hash_spills {
                    build_bytes * weights.spill_byte_weight
                } else {
                    0.0
                };
            let out_rows = (left_cost.estimated_rows.max(1.0) * right_cost.estimated_rows.max(1.0))
                .sqrt()
                .max(1.0);
            let mut best_op = PhysicalOp::HashJoin {
                join_type: *join_type,
                left: Box::new(left_op.clone()),
                right: Box::new(right_op.clone()),
                eq_keys: left_keys
                    .iter()
                    .cloned()
                    .zip(right_keys.iter().cloned())
                    .collect(),
                residual: residual.clone(),
                allow_spill: hash_spills,
            };
            let mut best_cost = PlanCost {
                total: hash_total,
                estimated_rows: out_rows,
                estimated_row_bytes: left_cost.estimated_row_bytes + right_cost.estimated_row_bytes,
                ipc_roundtrips: left_cost.ipc_roundtrips + right_cost.ipc_roundtrips,
                spills: left_cost.spills || right_cost.spills || hash_spills,
            };

            // Consider IndexNestedLoopJoin when right (or left for Inner join) is a base Scan with an eligible index prefix.
            let mut inlj_candidates: Vec<(&PhysicalOp, &PlanCost, &BoundRelation, bool)> =
                Vec::with_capacity(2);
            if let BoundLogicalOp::Scan { relation: r_rel } = right.as_ref() {
                inlj_candidates.push((&left_op, &left_cost, r_rel, false));
            }
            if *join_type == JoinType::Inner
                && let BoundLogicalOp::Scan { relation: l_rel } = left.as_ref()
            {
                inlj_candidates.push((&right_op, &right_cost, l_rel, true));
            }
            for (outer_op, outer_cost, inner_rel, use_left) in inlj_candidates {
                if let Some((index_id, prefix_cols, matched_outer_keys)) =
                    find_join_index_prefix(inner_rel, eq_keys, use_left)
                {
                    let inner_scan_cols = scan_columns_for_relation(inner_rel, required);
                    let probed_rows = limit_hint.map_or(outer_cost.estimated_rows, |lim| {
                        outer_cost.estimated_rows.min(f64::from(lim).max(1.0))
                    });
                    let batches = (probed_rows / weights.index_probe_batch_size)
                        .ceil()
                        .max(1.0);
                    let inlj_total = outer_cost.total
                        + batches * weights.ipc_roundtrip_weight
                        + probed_rows * weights.io_page_weight * 0.25;
                    if inlj_total < best_cost.total {
                        best_op = PhysicalOp::IndexNestedLoopJoin {
                            join_type: *join_type,
                            outer: Box::new(outer_op.clone()),
                            inner_relation: rel_ref(inner_rel),
                            inner_scan_columns: inner_scan_cols,
                            index_id,
                            outer_keys: matched_outer_keys,
                            inner_prefix_columns: prefix_cols,
                            residual: residual.clone(),
                        };
                        best_cost.total = inlj_total;
                        best_cost.ipc_roundtrips = outer_cost.ipc_roundtrips + batches;
                        best_cost.spills = outer_cost.spills;
                    }
                }
            }

            Ok((best_op, best_cost))
        }
        BoundLogicalOp::Aggregate {
            input,
            group_keys,
            aggregates,
            having,
        } => {
            let (child_op, child_cost) = plan_op(input, required, None, None, stats, weights)?;
            let g_cols: Vec<BoundColumn> = group_keys.iter().map(|k| k.column.clone()).collect();
            let p_aggs: Vec<BoundAggregateSpec> = aggregates
                .iter()
                .map(|a| BoundAggregateSpec {
                    kind: a.kind,
                    arg: a.arg.as_ref().map(|c| c.column.clone()),
                })
                .collect();
            let total_bytes = child_cost.estimated_rows * child_cost.estimated_row_bytes;
            let allow_spill = total_bytes > weights.in_memory_budget_bytes as f64;
            let agg_cost = child_cost.total
                + child_cost.estimated_rows * weights.cpu_row_weight
                + if allow_spill {
                    total_bytes * weights.spill_byte_weight
                } else {
                    0.0
                };
            Ok((
                PhysicalOp::HashAggregate {
                    input: Box::new(child_op),
                    group_keys: g_cols,
                    aggregates: p_aggs,
                    having: having.clone(),
                    allow_spill,
                },
                PlanCost {
                    total: agg_cost,
                    estimated_rows: (child_cost.estimated_rows * 0.2).max(1.0),
                    spills: child_cost.spills || allow_spill,
                    ..child_cost
                },
            ))
        }
        BoundLogicalOp::Delete {
            relation,
            predicate,
        } => {
            let (scan_op, scan_cost) = plan_filtered_scan(
                relation,
                Some(predicate),
                required,
                None,
                None,
                stats,
                weights,
            )?;
            Ok((
                PhysicalOp::Delete {
                    relation: rel_ref(relation),
                    input: Box::new(scan_op),
                },
                scan_cost,
            ))
        }
        BoundLogicalOp::Update {
            relation,
            assignments,
            predicate,
        } => {
            let (scan_op, scan_cost) = plan_filtered_scan(
                relation,
                Some(predicate),
                required,
                None,
                None,
                stats,
                weights,
            )?;
            let p_assigns = assignments
                .iter()
                .map(|a| PhysicalUpdateAssignment {
                    column: a.column.clone(),
                    value: a.value,
                })
                .collect();
            Ok((
                PhysicalOp::Update {
                    relation: rel_ref(relation),
                    input: Box::new(scan_op),
                    assignments: p_assigns,
                },
                scan_cost,
            ))
        }
    }
}

fn plan_filtered_scan(
    relation: &BoundRelation,
    predicate: Option<&BoundExpr>,
    required: &HashSet<(u16, [u8; 16])>,
    desired_order: Option<&[BoundOrderItem]>,
    limit_hint: Option<u32>,
    stats: &StatsCatalog,
    weights: &CostWeights,
) -> Result<(PhysicalOp, PlanCost), PlanError> {
    let scan_columns = scan_columns_for_relation(relation, required);
    let rel_stats = stats.get(relation.relation_index);
    let base_rows = rel_stats
        .map(|s| s.visible_rows as f64)
        .unwrap_or(weights.unknown_default_rows);
    let row_bytes = rel_stats
        .filter(|s| s.avg_authorized_row_bytes > 0)
        .map(|s| f64::from(s.avg_authorized_row_bytes))
        .unwrap_or(weights.unknown_default_row_bytes);

    if base_rows == 0.0 && rel_stats.is_some_and(|s| s.is_exact) {
        return Ok((
            PhysicalOp::EmptyResult {
                output_columns: scan_columns,
            },
            PlanCost {
                total: 0.0,
                estimated_rows: 0.0,
                estimated_row_bytes: 0.0,
                ipc_roundtrips: 0.0,
                spills: false,
            },
        ));
    }

    let pages = ((base_rows * row_bytes) / weights.page_bytes)
        .ceil()
        .max(1.0);
    let seq_cost_base = pages * weights.io_page_weight
        + pages * weights.ipc_roundtrip_weight
        + base_rows * weights.cpu_row_weight;

    let mut best_op = PhysicalOp::SeqScan {
        relation: rel_ref(relation),
        scan_columns: scan_columns.clone(),
    };
    let mut best_cost = PlanCost {
        total: seq_cost_base,
        estimated_rows: base_rows,
        estimated_row_bytes: row_bytes,
        ipc_roundtrips: pages,
        spills: false,
    };

    // Check CoveringIndexScan when no predicate or range is needed, or as a base scan replacement.
    if let Some(cov_idx) = find_covering_index(relation, &scan_columns) {
        let cov_pages = (pages * 0.4).max(1.0);
        let cov_cost = cov_pages * weights.io_page_weight
            + cov_pages * weights.ipc_roundtrip_weight
            + base_rows * weights.cpu_row_weight;
        if cov_cost < best_cost.total {
            best_op = PhysicalOp::CoveringIndexScan {
                relation: rel_ref(relation),
                scan_columns: scan_columns.clone(),
                index_id: cov_idx,
                range_column: None,
                lower: None,
                upper: None,
            };
            best_cost.total = cov_cost;
            best_cost.ipc_roundtrips = cov_pages;
        }
    }

    // Check IndexOrderedScan if desired_order matches a leading authorized index key.
    if let Some(order_by) = desired_order
        && let Some((index_id, order_cols, desc)) = find_ordered_index(relation, order_by)
    {
        let est_rows = limit_hint.map_or(base_rows, |l| base_rows.min(f64::from(l)));
        let ord_pages = ((est_rows * row_bytes) / weights.page_bytes)
            .ceil()
            .max(1.0);
        let ord_cost = ord_pages * weights.io_page_weight
            + ord_pages * weights.ipc_roundtrip_weight
            + est_rows * weights.cpu_row_weight;
        if ord_cost <= best_cost.total {
            best_op = PhysicalOp::IndexOrderedScan {
                relation: rel_ref(relation),
                scan_columns: scan_columns.clone(),
                index_id,
                order_columns: order_cols,
                descending: desc,
                limit_hint,
            };
            best_cost.total = ord_cost;
            best_cost.estimated_rows = est_rows;
            best_cost.ipc_roundtrips = ord_pages;
        }
    }

    let Some(pred) = predicate else {
        return Ok((best_op, best_cost));
    };

    // Split AND conjuncts so indexable predicates can drive an index scan while
    // keeping residual predicates in a Filter node.
    let conjuncts = split_conjuncts(pred);
    let mut residual_conjuncts = conjuncts.clone();

    if let Some((idx_op, idx_sel, consumed)) =
        select_best_index_access(relation, &scan_columns, &conjuncts, rel_stats)
    {
        let est_rows = (base_rows * idx_sel).max(1.0);
        let idx_pages = ((est_rows * row_bytes) / weights.page_bytes)
            .ceil()
            .max(1.0);
        let idx_cost = idx_pages * weights.io_page_weight
            + weights.ipc_roundtrip_weight
            + est_rows * weights.cpu_row_weight;
        if idx_cost < best_cost.total {
            best_op = idx_op;
            best_cost.total = idx_cost;
            best_cost.estimated_rows = est_rows;
            best_cost.ipc_roundtrips = 1.0;
            residual_conjuncts = conjuncts
                .iter()
                .enumerate()
                .filter(|(idx, _)| !consumed.contains(idx))
                .map(|(_, c)| c.clone())
                .collect();
        }
    }

    if let Some(full_pred) = combine_conjuncts(std::mem::take(&mut residual_conjuncts)) {
        let sel = estimate_expr_selectivity(&full_pred, stats);
        best_cost.total += best_cost.estimated_rows * weights.cpu_row_weight;
        best_cost.estimated_rows = (best_cost.estimated_rows * sel).max(1.0);
        best_op = PhysicalOp::Filter {
            input: Box::new(best_op),
            predicate: full_pred,
        };
    }

    Ok((best_op, best_cost))
}

fn select_best_index_access(
    relation: &BoundRelation,
    scan_columns: &[BoundColumn],
    conjuncts: &[BoundExpr],
    rel_stats: Option<&AuthorizedRelationStats>,
) -> Option<(PhysicalOp, f64, Vec<usize>)> {
    // 1. Unique point probe on any conjunct `col = literal/param`.
    for (i, c) in conjuncts.iter().enumerate() {
        if let BoundExpr::Compare {
            op: Comparison::Equal,
            left,
            right,
        } = c
            && let Some((col, val)) = extract_col_and_value(left, right)
            && let Some(path) = relation
                .unique_access_paths
                .iter()
                .find(|p| p.index_id != [0; 16] && p.column_ids.as_slice() == [col.column_id])
        {
            return Some((
                PhysicalOp::IndexPointScan {
                    relation: rel_ref(relation),
                    scan_columns: scan_columns.to_vec(),
                    index_id: path.index_id,
                    key_columns: vec![col],
                    key_values: vec![val],
                },
                0.001,
                vec![i],
            ));
        }
    }

    // 2. Leading-key range scan on `<, <=, >, >=, =`.
    for (i, c) in conjuncts.iter().enumerate() {
        if let BoundExpr::Compare { op, left, right } = c
            && *op != Comparison::NotEqual
            && let Some((col, val)) = extract_col_and_value(left, right)
            && let Some(index_id) = find_leading_index(relation, col.column_id)
        {
            let (mut lower, mut upper) = match op {
                Comparison::Equal => (
                    Some(IndexBound {
                        value: val.clone(),
                        inclusive: true,
                    }),
                    Some(IndexBound {
                        value: val,
                        inclusive: true,
                    }),
                ),
                Comparison::Greater | Comparison::GreaterOrEqual => (
                    Some(IndexBound {
                        value: val,
                        inclusive: *op == Comparison::GreaterOrEqual,
                    }),
                    None,
                ),
                Comparison::Less | Comparison::LessOrEqual => (
                    None,
                    Some(IndexBound {
                        value: val,
                        inclusive: *op == Comparison::LessOrEqual,
                    }),
                ),
                Comparison::NotEqual => continue,
            };
            let mut consumed = vec![i];
            for (j, other) in conjuncts.iter().enumerate().skip(i + 1) {
                if let BoundExpr::Compare {
                    op: o_op,
                    left: o_left,
                    right: o_right,
                } = other
                    && let Some((o_col, o_val)) = extract_col_and_value(o_left, o_right)
                    && o_col.column_id == col.column_id
                {
                    match o_op {
                        Comparison::Greater | Comparison::GreaterOrEqual if lower.is_none() => {
                            lower = Some(IndexBound {
                                value: o_val,
                                inclusive: *o_op == Comparison::GreaterOrEqual,
                            });
                            consumed.push(j);
                        }
                        Comparison::Less | Comparison::LessOrEqual if upper.is_none() => {
                            upper = Some(IndexBound {
                                value: o_val,
                                inclusive: *o_op == Comparison::LessOrEqual,
                            });
                            consumed.push(j);
                        }
                        _ => {}
                    }
                }
            }
            let sel = estimate_range_col_selectivity(rel_stats, col.column_id);
            return Some((
                PhysicalOp::IndexRangeScan {
                    relation: rel_ref(relation),
                    scan_columns: scan_columns.to_vec(),
                    index_id,
                    prefix_columns: Vec::new(),
                    prefix_values: Vec::new(),
                    range_column: Some(col),
                    lower,
                    upper,
                },
                sel,
                consumed,
            ));
        }
    }

    None
}

fn extract_col_and_value(left: &BoundExpr, right: &BoundExpr) -> Option<(BoundColumn, BoundExpr)> {
    match (left, right) {
        (BoundExpr::Column(c), val) if !matches!(val, BoundExpr::Column(_)) => {
            Some((c.column.clone(), val.clone()))
        }
        (val, BoundExpr::Column(c)) if !matches!(val, BoundExpr::Column(_)) => {
            Some((c.column.clone(), val.clone()))
        }
        _ => None,
    }
}

fn find_leading_index(relation: &BoundRelation, col_id: [u8; 16]) -> Option<[u8; 16]> {
    relation
        .unique_access_paths
        .iter()
        .map(|p| (p.index_id, &p.column_ids))
        .chain(
            relation
                .range_access_paths
                .iter()
                .map(|p| (p.index_id, &p.column_ids)),
        )
        .find(|(id, cols)| *id != [0; 16] && cols.first() == Some(&col_id))
        .map(|(id, _)| id)
}

fn find_covering_index(relation: &BoundRelation, scan_columns: &[BoundColumn]) -> Option<[u8; 16]> {
    relation
        .unique_access_paths
        .iter()
        .map(|p| (p.index_id, &p.column_ids))
        .chain(
            relation
                .range_access_paths
                .iter()
                .map(|p| (p.index_id, &p.column_ids)),
        )
        .find(|(id, idx_cols)| {
            *id != [0; 16]
                && scan_columns
                    .iter()
                    .all(|sc| idx_cols.contains(&sc.column_id))
        })
        .map(|(id, _)| id)
}

fn find_ordered_index(
    relation: &BoundRelation,
    order_by: &[BoundOrderItem],
) -> Option<([u8; 16], Vec<BoundColumn>, bool)> {
    let first = order_by.first()?;
    let BoundOrderTarget::Column(col_ref) = &first.target else {
        return None;
    };
    if col_ref.relation_index != relation.relation_index {
        return None;
    }
    let index_id = find_leading_index(relation, col_ref.column.column_id)?;
    Some((index_id, vec![col_ref.column.clone()], first.descending))
}

fn find_join_index_prefix(
    relation: &BoundRelation,
    eq_keys: &[(
        crate::optimizer_ir::expr::BoundColumnRef,
        crate::optimizer_ir::expr::BoundColumnRef,
    )],
    use_left: bool,
) -> Option<([u8; 16], Vec<BoundColumn>, Vec<BoundColumn>)> {
    let mut best: Option<([u8; 16], Vec<BoundColumn>, Vec<BoundColumn>)> = None;
    for (id, idx_cols) in relation
        .unique_access_paths
        .iter()
        .map(|p| (p.index_id, &p.column_ids))
        .chain(
            relation
                .range_access_paths
                .iter()
                .map(|p| (p.index_id, &p.column_ids)),
        )
    {
        if id == [0; 16] {
            continue;
        }
        let mut inner_prefix = Vec::new();
        let mut outer_keys = Vec::new();
        for col_id in idx_cols.iter().take(8) {
            let found = if use_left {
                eq_keys
                    .iter()
                    .find(|(l, _)| l.column.column_id == *col_id)
                    .map(|(l, r)| (l, r))
            } else {
                eq_keys
                    .iter()
                    .find(|(_, r)| r.column.column_id == *col_id)
                    .map(|(l, r)| (r, l))
            };
            let Some((inner_col, outer_col)) = found else {
                break;
            };
            inner_prefix.push(inner_col.column.clone());
            outer_keys.push(outer_col.column.clone());
        }
        if !inner_prefix.is_empty()
            && best
                .as_ref()
                .is_none_or(|(_, b_pref, _)| inner_prefix.len() > b_pref.len())
        {
            best = Some((id, inner_prefix, outer_keys));
        }
    }
    best
}

fn is_ordered_by(op: &PhysicalOp, order_by: &[BoundOrderItem]) -> bool {
    if order_by.len() != 1 {
        return false;
    }
    let BoundOrderTarget::Column(target) = &order_by[0].target else {
        return false;
    };
    match op {
        PhysicalOp::IndexOrderedScan {
            order_columns,
            descending,
            ..
        } => order_columns.first() == Some(&target.column) && *descending == order_by[0].descending,
        PhysicalOp::Filter { input, .. } | PhysicalOp::Project { input, .. } => {
            is_ordered_by(input, order_by)
        }
        _ => false,
    }
}

fn estimate_range_col_selectivity(
    rel_stats: Option<&AuthorizedRelationStats>,
    col_id: [u8; 16],
) -> f64 {
    let Some(rs) = rel_stats else {
        return 0.25;
    };
    let Some(cs) = rs.columns.iter().find(|c| c.column_id == col_id) else {
        return 0.25;
    };
    cs.histogram
        .as_ref()
        .map_or(0.25, |h| h.estimate_range_selectivity(None, None))
}

fn estimate_expr_selectivity(expr: &BoundExpr, stats: &StatsCatalog) -> f64 {
    match expr {
        BoundExpr::Compare {
            op: Comparison::Equal,
            left,
            right,
        } => {
            if let BoundExpr::Column(c) = left.as_ref()
                && let Some(rs) = stats.get(c.relation_index)
                && let Some(cs) = rs
                    .columns
                    .iter()
                    .find(|cs| cs.column_id == c.column.column_id)
                && let Some(h) = &cs.histogram
            {
                return h.estimate_equality_selectivity(cs.ndv);
            }
            let _ = right;
            0.1
        }
        BoundExpr::And(children) => children
            .iter()
            .fold(1.0, |acc, c| acc * estimate_expr_selectivity(c, stats)),
        BoundExpr::Or(children) => {
            let p_none = children.iter().fold(1.0, |acc, c| {
                acc * (1.0 - estimate_expr_selectivity(c, stats))
            });
            (1.0 - p_none).clamp(0.0, 1.0)
        }
        _ => 0.5,
    }
}

fn rel_ref(rel: &BoundRelation) -> RelationReference {
    RelationReference {
        relation_index: rel.relation_index,
        relation_name: rel.relation_name.clone(),
        relation_id: rel.relation_id,
        table_epoch: rel.table_epoch,
    }
}

fn scan_columns_for_relation(
    rel: &BoundRelation,
    required: &HashSet<(u16, [u8; 16])>,
) -> Vec<BoundColumn> {
    let mut cols: Vec<BoundColumn> = rel
        .columns
        .iter()
        .filter(|c| required.contains(&(rel.relation_index, c.column_id)))
        .cloned()
        .collect();
    if cols.is_empty()
        && let Some(first) = rel.columns.first()
    {
        cols.push(first.clone());
    }
    cols.sort_by_key(|c| c.ordinal);
    cols
}

fn output_columns_for_op(
    op: &BoundLogicalOp,
    required: &HashSet<(u16, [u8; 16])>,
) -> Vec<BoundColumn> {
    match op {
        BoundLogicalOp::Scan { relation } => scan_columns_for_relation(relation, required),
        BoundLogicalOp::Project { columns, .. } => {
            columns.iter().map(|c| c.column.clone()).collect()
        }
        BoundLogicalOp::Filter { input, .. }
        | BoundLogicalOp::Sort { input, .. }
        | BoundLogicalOp::Limit { input, .. } => output_columns_for_op(input, required),
        BoundLogicalOp::Join { left, .. } => output_columns_for_op(left, required),
        BoundLogicalOp::Aggregate { group_keys, .. } => {
            group_keys.iter().map(|c| c.column.clone()).collect()
        }
        BoundLogicalOp::Delete { relation, .. } | BoundLogicalOp::Update { relation, .. } => {
            scan_columns_for_relation(relation, required)
        }
    }
}

fn approx_log2(value: f64) -> f64 {
    if !value.is_finite() || value <= 1.0 {
        return 1.0;
    }
    let exp = ((value.to_bits() >> 52) & 0x7ff) as i32 - 1023;
    (exp.max(1)) as f64
}

fn to_physical_order(order_by: &[BoundOrderItem]) -> Vec<PhysicalOrderItem> {
    order_by
        .iter()
        .map(|o| PhysicalOrderItem {
            target: o.target.clone(),
            descending: o.descending,
            nulls_first: o.nulls_first,
        })
        .collect()
}
