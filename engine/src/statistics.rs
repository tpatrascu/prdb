//! Scoped summaries exposed to the optimizer for one active invocation.

/// Maximum row candidates inspected by one on-demand estimate.
pub const MAX_ESTIMATE_CANDIDATES: usize = 16_384;

/// Result of a bounded authorized scan summary. Unknown estimates contain no
/// partial counts, so callers can use fixed heuristics without learning how
/// much of a population was inspected.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthorizedScanEstimate {
    Exact(AuthorizedScanSummary),
    Unknown,
}

/// Counts over rows returned by the invocation's authorized projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedScanSummary {
    pub visible_rows: u64,
    /// Entries follow the requested column order. No values or widths are kept.
    pub columns: Vec<ColumnPopulation>,
}

/// Cell-state counts within the visible-row population for one requested field.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ColumnPopulation {
    pub readable_values: u64,
    pub nulls: u64,
    pub redacted: u64,
}
