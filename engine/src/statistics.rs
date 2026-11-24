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
}
