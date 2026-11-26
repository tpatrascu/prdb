//! Equivalence-preserving logical rewrites: constant folding, AND-conjunct
//! splitting, and bottom-up column pruning.

use crate::optimizer_ir::expr::{BoundExpr, Comparison};
use std::collections::HashSet;

/// Split a `BoundExpr` into top-level `AND` conjuncts.
pub fn split_conjuncts(expr: &BoundExpr) -> Vec<BoundExpr> {
    let mut out = Vec::new();
    fn walk(e: &BoundExpr, out: &mut Vec<BoundExpr>) {
        match e {
            BoundExpr::And(children) => {
                for c in children {
                    walk(c, out);
                }
            }
            other => out.push(other.clone()),
        }
    }
    walk(expr, &mut out);
    out
}

/// Recombine conjuncts into a single `BoundExpr` (or `None` if empty).
pub fn combine_conjuncts(mut conjuncts: Vec<BoundExpr>) -> Option<BoundExpr> {
    match conjuncts.len() {
        0 => None,
        1 => Some(conjuncts.remove(0)),
        _ => Some(BoundExpr::And(conjuncts)),
    }
}

/// Evaluate constant-only Boolean expressions where safe (preserving SQL
/// 3-valued logic and never dropping column references that could trigger
/// redaction errors).
pub fn fold_constant_bool(expr: &BoundExpr) -> Option<bool> {
    match expr {
        BoundExpr::Boolean(b) => Some(*b),
        BoundExpr::Compare { op, left, right } => match (left.as_ref(), right.as_ref()) {
            (BoundExpr::Integer(a), BoundExpr::Integer(b)) => Some(match op {
                Comparison::Equal => a == b,
                Comparison::NotEqual => a != b,
                Comparison::Less => a < b,
                Comparison::LessOrEqual => a <= b,
                Comparison::Greater => a > b,
                Comparison::GreaterOrEqual => a >= b,
            }),
            (BoundExpr::Boolean(a), BoundExpr::Boolean(b)) => match op {
                Comparison::Equal => Some(a == b),
                Comparison::NotEqual => Some(a != b),
                _ => None,
            },
            _ => None,
        },
        BoundExpr::Not(inner) => fold_constant_bool(inner).map(|b| !b),
        BoundExpr::And(children) => {
            let mut all_true = true;
            for c in children {
                match fold_constant_bool(c) {
                    Some(false) => return Some(false),
                    Some(true) => {}
                    None => all_true = false,
                }
            }
            all_true.then_some(true)
        }
        BoundExpr::Or(children) => {
            let mut all_false = true;
            for c in children {
                match fold_constant_bool(c) {
                    Some(true) => return Some(true),
                    Some(false) => {}
                    None => all_false = false,
                }
            }
            all_false.then_some(false)
        }
        _ => None,
    }
}

/// Collect all `(relation_index, column_id)` pairs referenced in `expr`.
pub fn collect_expr_column_ids(expr: &BoundExpr, out: &mut HashSet<(u16, [u8; 16])>) {
    match expr {
        BoundExpr::Column(c) => {
            out.insert((c.relation_index, c.column.column_id));
        }
        BoundExpr::Compare { left, right, .. } => {
            collect_expr_column_ids(left, out);
            collect_expr_column_ids(right, out);
        }
        BoundExpr::And(children) | BoundExpr::Or(children) => {
            for c in children {
                collect_expr_column_ids(c, out);
            }
        }
        BoundExpr::Not(inner) | BoundExpr::IsNull { expr: inner, .. } => {
            collect_expr_column_ids(inner, out);
        }
        BoundExpr::InList { expr, items, .. } => {
            collect_expr_column_ids(expr, out);
            for item in items {
                collect_expr_column_ids(item, out);
            }
        }
        _ => {}
    }
}
