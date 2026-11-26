//! Authorized-view cost model and composable physical planner.

pub mod planner;
pub mod rewrite;
#[cfg(test)]
mod tests;

pub use planner::optimize_bound_request;

pub const SMALL_POPULATION_SCAN_ROWS: u64 = 4;
pub const JOIN_POPULATION_ESTIMATE_MAX_ROWS: u64 = 16_384;

use crate::statistics::AuthorizedRelationStats;
use std::collections::HashMap;

/// Cost weights calibrated to `prdb`'s storage-page, IPC-roundtrip, CPU, and
/// spill-broker bottlenecks.
#[derive(Clone, Copy, Debug)]
pub struct CostWeights {
    pub io_page_weight: f64,
    pub ipc_roundtrip_weight: f64,
    pub cpu_row_weight: f64,
    pub spill_byte_weight: f64,
    pub page_bytes: f64,
    pub in_memory_budget_bytes: u64,
    pub index_probe_batch_size: f64,
    pub unknown_default_rows: f64,
    pub unknown_default_row_bytes: f64,
}

impl Default for CostWeights {
    fn default() -> Self {
        Self {
            io_page_weight: 10.0,
            ipc_roundtrip_weight: 25.0,
            cpu_row_weight: 0.05,
            spill_byte_weight: 0.002,
            page_bytes: 8192.0,
            in_memory_budget_bytes: 1024 * 1024, // 1 MiB default
            index_probe_batch_size: 128.0,
            unknown_default_rows: 1_000.0,
            unknown_default_row_bytes: 64.0,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PlanCost {
    pub total: f64,
    pub estimated_rows: f64,
    pub estimated_row_bytes: f64,
    pub ipc_roundtrips: f64,
    pub spills: bool,
}

/// Authorized-view statistics keyed by `relation_index`. Missing entries
/// represent `Unknown` (e.g., value-dependent row policies or over-budget).
#[derive(Clone, Debug, Default)]
pub struct StatsCatalog {
    pub relations: HashMap<u16, AuthorizedRelationStats>,
}

impl StatsCatalog {
    pub fn get(&self, relation_index: u16) -> Option<&AuthorizedRelationStats> {
        self.relations.get(&relation_index)
    }
}
