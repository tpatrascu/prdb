//! Authorization-scoped optimizer estimates and protected trusted statistics.

/// Default committed rows inspected by one maintenance collection pass.
pub const MAX_COMMITTED_POPULATION_ROWS: u64 = 16_384;
/// Maximum committed rows allowed in an exact protected population record.
/// This is an implementation envelope, not a production maintenance target.
pub const MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS: u64 = 1_048_576;
/// Maximum exact population per side considered by one join-cardinality estimate.
pub const MAX_JOIN_ESTIMATE_ROWS: u64 = 16_384;
/// Maximum encoded row bytes read across both sides of one join estimate.
pub const MAX_JOIN_ESTIMATE_BYTES: usize = 8 * 1024 * 1024;
/// Maximum encoded composite join key retained by the bounded estimator.
pub const MAX_JOIN_ESTIMATE_KEY_BYTES: usize = 4096;
const COMMITTED_POPULATION_MAGIC: &[u8; 4] = b"CPR1";
const COMMITTED_POPULATION_RECORD_LEN: usize = 21;
/// Internal namespace in the protected `stats_v1` table.
pub(crate) const COMMITTED_POPULATION_KEY_PREFIX: &[u8] = b"statistics:committed-population:v1:";

/// Public maintenance outcome. It deliberately omits the global row count;
/// exact counts stay in the protected store for trusted internal consumers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsCollectionOutcome {
    Exact,
    Unknown,
}

/// Protected committed population record stored in the internal stats table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CommittedPopulationRecord {
    Exact {
        table_id: [u8; 16],
        snapshot_sequence: u64,
        rows: u64,
    },
    Unknown {
        table_id: [u8; 16],
        snapshot_sequence: u64,
    },
}

/// A malformed or unavailable protected statistics record.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatisticsRecordError {
    InvalidLength,
    InvalidMagic,
    InvalidStatus,
    InvalidValue,
}

impl std::fmt::Display for StatisticsRecordError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid committed population record: {self:?}")
    }
}

impl std::error::Error for StatisticsRecordError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatisticsMaintenanceError {
    InvalidRequest,
    Busy,
    Storage(String),
}

impl std::fmt::Display for StatisticsMaintenanceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "statistics maintenance failed: {self:?}")
    }
}

impl std::error::Error for StatisticsMaintenanceError {}

impl CommittedPopulationRecord {
    pub(crate) fn table_id(&self) -> [u8; 16] {
        match self {
            Self::Exact { table_id, .. } | Self::Unknown { table_id, .. } => *table_id,
        }
    }

    pub(crate) fn snapshot_sequence(&self) -> u64 {
        match self {
            Self::Exact {
                snapshot_sequence, ..
            }
            | Self::Unknown {
                snapshot_sequence, ..
            } => *snapshot_sequence,
        }
    }

    pub(crate) fn encode_record(self) -> [u8; COMMITTED_POPULATION_RECORD_LEN] {
        let mut bytes = [0; COMMITTED_POPULATION_RECORD_LEN];
        bytes[..4].copy_from_slice(COMMITTED_POPULATION_MAGIC);
        bytes[4..12].copy_from_slice(&self.snapshot_sequence().to_le_bytes());
        match self {
            Self::Exact { rows, .. } => {
                bytes[12] = 1;
                bytes[13..21].copy_from_slice(&rows.to_le_bytes());
            }
            Self::Unknown { .. } => bytes[12] = 0,
        }
        bytes
    }

    pub(crate) fn decode_record(
        table_id: [u8; 16],
        bytes: &[u8],
    ) -> Result<Self, StatisticsRecordError> {
        if bytes.len() != COMMITTED_POPULATION_RECORD_LEN {
            return Err(StatisticsRecordError::InvalidLength);
        }
        if &bytes[..4] != COMMITTED_POPULATION_MAGIC {
            return Err(StatisticsRecordError::InvalidMagic);
        }
        let snapshot_sequence = u64::from_le_bytes(bytes[4..12].try_into().expect("fixed range"));
        let rows = u64::from_le_bytes(bytes[13..21].try_into().expect("fixed range"));
        match bytes[12] {
            0 if rows == 0 => Ok(Self::Unknown {
                table_id,
                snapshot_sequence,
            }),
            1 if rows <= MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS => Ok(Self::Exact {
                table_id,
                snapshot_sequence,
                rows,
            }),
            0 | 1 => Err(StatisticsRecordError::InvalidValue),
            _ => Err(StatisticsRecordError::InvalidStatus),
        }
    }
}

/// Operational default row candidates inspected by one on-demand estimate.
pub const DEFAULT_ESTIMATE_CANDIDATES: usize = 16_384;
/// Compatibility name for the default estimate candidate limit.
pub const MAX_ESTIMATE_CANDIDATES: usize = DEFAULT_ESTIMATE_CANDIDATES;
/// Maximum configurable row candidates inspected by one on-demand estimate.
/// This is an implementation envelope, not a production workload target.
pub const MAX_CONFIGURABLE_ESTIMATE_CANDIDATES: usize = 1_048_576;
/// Maximum visible population for exact planner predicate estimates. This
/// matches the implementation envelope for the per-estimate candidate budget.
pub const MAX_PREDICATE_ESTIMATE_ROWS: u64 = MAX_CONFIGURABLE_ESTIMATE_CANDIDATES as u64;

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

/// Bounded equi-depth histogram over readable, orderable scalar values in the
/// invocation's authorized view.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EquiDepthHistogram {
    pub sample_rows: u64,
    /// Sorted bucket boundary values (at most 16 buckets -> up to 17 boundaries).
    pub boundaries: Vec<crate::tuple::ScalarValue>,
}

impl EquiDepthHistogram {
    pub const MAX_BUCKETS: usize = 16;

    /// Estimate equality selectivity in `[0.0, 1.0]` using `1 / max(ndv, 1)`
    /// clamped by bucket presence if boundaries exist.
    pub fn estimate_equality_selectivity(&self, ndv: Option<u64>) -> f64 {
        if self.sample_rows == 0 {
            return 0.0;
        }
        if let Some(d) = ndv
            && d > 0
        {
            return (1.0 / (d as f64)).clamp(0.0, 1.0);
        }
        0.1
    }

    /// Estimate range selectivity in `(0.0, 1.0]` for `col >= lower` / `col <= upper`.
    pub fn estimate_range_selectivity(
        &self,
        lower: Option<(&crate::tuple::ScalarValue, bool)>,
        upper: Option<(&crate::tuple::ScalarValue, bool)>,
    ) -> f64 {
        if self.boundaries.len() < 2 {
            return 0.25;
        }
        let buckets = (self.boundaries.len() - 1) as f64;
        let lo_pos = match lower {
            None => 0.0,
            Some((val, _)) => self.fractional_position(val),
        };
        let hi_pos = match upper {
            None => 1.0,
            Some((val, _)) => self.fractional_position(val),
        };
        if hi_pos < lo_pos {
            return (1.0 / (self.sample_rows.max(1) as f64)).min(0.01);
        }
        let span = (hi_pos - lo_pos).max(1.0 / (buckets * 2.0));
        span.clamp(0.0, 1.0)
    }

    fn fractional_position(&self, target: &crate::tuple::ScalarValue) -> f64 {
        let n = self.boundaries.len();
        if n < 2 {
            return 0.5;
        }
        let mut idx = 0usize;
        while idx + 1 < n
            && compare_scalar_values(&self.boundaries[idx + 1], target)
                == Some(std::cmp::Ordering::Less)
        {
            idx += 1;
        }
        (idx as f64) / ((n - 1) as f64)
    }
}

pub(crate) fn compare_scalar_values(
    left: &crate::tuple::ScalarValue,
    right: &crate::tuple::ScalarValue,
) -> Option<std::cmp::Ordering> {
    use crate::tuple::ScalarValue::*;
    match (left, right) {
        (Boolean(a), Boolean(b)) => Some(a.cmp(b)),
        (SmallInt(a), SmallInt(b)) => Some(a.cmp(b)),
        (Integer(a), Integer(b)) => Some(a.cmp(b)),
        (BigInt(a), BigInt(b)) => Some(a.cmp(b)),
        (Decimal(a), Decimal(b)) => Some(a.cmp(b)),
        (Text(a), Text(b)) | (Varchar(a), Varchar(b)) => Some(a.cmp(b)),
        (Bytea(a), Bytea(b)) => Some(a.cmp(b)),
        (Uuid(a), Uuid(b)) => Some(a.cmp(b)),
        (Date(a), Date(b)) => Some(a.cmp(b)),
        (TimestampTz(a), TimestampTz(b)) => Some(a.cmp(b)),
        _ => None,
    }
}

/// Per-column authorized-view statistics. `ndv` and `histogram` are omitted
/// (`None`) whenever any visible row has a redacted cell for this column.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedColumnStats {
    pub column_id: [u8; 16],
    pub ndv: Option<u64>,
    pub null_fraction_ppm: Option<u32>,
    pub redacted_fraction_ppm: u32,
    pub histogram: Option<EquiDepthHistogram>,
}

/// Authorized-view relation statistics used by the cost-based optimizer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthorizedRelationStats {
    pub visible_rows: u64,
    pub is_exact: bool,
    pub avg_authorized_row_bytes: u32,
    pub columns: Vec<AuthorizedColumnStats>,
}

/// Cache key proving identical effective authority, policy/grant dependencies,
/// schema version, and data snapshot for safe cross-invocation reuse.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct PolicyEquivalenceKey {
    pub snapshot_sequence: u64,
    pub schema_version: u64,
    pub relation_id: [u8; 16],
    pub effective_role_hash: [u8; 32],
    pub policy_dependency_hash: [u8; 32],
    pub grant_dependency_hash: [u8; 32],
}

/// Bounded cache of authorized-view relation statistics keyed by
/// `PolicyEquivalenceKey`. Only clean-overlay, literal-policy or
/// policy-equivalent entries may be inserted.
#[derive(Default)]
pub struct PolicyEquivalentStatsCache {
    entries: std::collections::HashMap<PolicyEquivalenceKey, AuthorizedRelationStats>,
}

impl PolicyEquivalentStatsCache {
    pub const MAX_ENTRIES: usize = 256;

    pub fn get(&self, key: &PolicyEquivalenceKey) -> Option<&AuthorizedRelationStats> {
        self.entries.get(key)
    }

    pub fn insert(&mut self, key: PolicyEquivalenceKey, stats: AuthorizedRelationStats) {
        if self.entries.len() >= Self::MAX_ENTRIES
            && !self.entries.contains_key(&key)
            && let Some(evict) = self.entries.keys().next().cloned()
        {
            self.entries.remove(&evict);
        }
        self.entries.insert(key, stats);
    }
}

#[cfg(test)]
mod committed_population_record_tests {
    use super::{
        CommittedPopulationRecord, MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS,
        StatisticsRecordError,
    };

    #[test]
    fn committed_population_record_round_trips_exact_and_unknown() {
        let exact = CommittedPopulationRecord::Exact {
            table_id: [7; 16],
            snapshot_sequence: 19,
            rows: 128,
        };
        assert_eq!(
            CommittedPopulationRecord::decode_record([7; 16], &exact.encode_record()),
            Ok(exact)
        );

        let unknown = CommittedPopulationRecord::Unknown {
            table_id: [8; 16],
            snapshot_sequence: 20,
        };
        assert_eq!(
            CommittedPopulationRecord::decode_record([8; 16], &unknown.encode_record()),
            Ok(unknown)
        );
    }

    #[test]
    fn committed_population_record_rejects_noncanonical_and_oversized_counts() {
        let exact = CommittedPopulationRecord::Exact {
            table_id: [7; 16],
            snapshot_sequence: 19,
            rows: MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS + 1,
        };
        let mut bytes = exact.encode_record();
        assert_eq!(
            CommittedPopulationRecord::decode_record([7; 16], &bytes),
            Err(StatisticsRecordError::InvalidValue)
        );
        bytes[12] = 3;
        assert_eq!(
            CommittedPopulationRecord::decode_record([7; 16], &bytes),
            Err(StatisticsRecordError::InvalidStatus)
        );
    }

    #[test]
    fn equi_depth_histogram_selectivity_and_policy_cache_isolation() {
        use super::{
            AuthorizedColumnStats, AuthorizedRelationStats, EquiDepthHistogram,
            PolicyEquivalenceKey, PolicyEquivalentStatsCache,
        };
        use crate::tuple::ScalarValue;

        let hist = EquiDepthHistogram {
            sample_rows: 100,
            boundaries: vec![
                ScalarValue::Integer(0),
                ScalarValue::Integer(25),
                ScalarValue::Integer(50),
                ScalarValue::Integer(75),
                ScalarValue::Integer(100),
            ],
        };
        assert!((hist.estimate_equality_selectivity(Some(50)) - 0.02).abs() < 1e-6);
        let range_sel = hist.estimate_range_selectivity(
            Some((&ScalarValue::Integer(25), true)),
            Some((&ScalarValue::Integer(75), true)),
        );
        assert!(range_sel > 0.25 && range_sel <= 0.75);

        let key_a = PolicyEquivalenceKey {
            snapshot_sequence: 10,
            schema_version: 1,
            relation_id: [1; 16],
            effective_role_hash: [2; 32],
            policy_dependency_hash: [3; 32],
            grant_dependency_hash: [4; 32],
        };
        let mut key_b = key_a.clone();
        key_b.effective_role_hash = [9; 32];

        let mut cache = PolicyEquivalentStatsCache::default();
        let stats = AuthorizedRelationStats {
            visible_rows: 100,
            is_exact: true,
            avg_authorized_row_bytes: 32,
            columns: vec![AuthorizedColumnStats {
                column_id: [5; 16],
                ndv: Some(50),
                null_fraction_ppm: Some(0),
                redacted_fraction_ppm: 0,
                histogram: Some(hist),
            }],
        };
        cache.insert(key_a.clone(), stats.clone());
        assert_eq!(cache.get(&key_a), Some(&stats));
        assert_eq!(cache.get(&key_b), None);
    }
}
