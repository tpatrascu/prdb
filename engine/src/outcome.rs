//! Durable terminal invocation outcome value.
const MAGIC: &[u8; 4] = b"OUT1";
const FORMAT_VERSION: u16 = 1;
const ENCODED_LEN: usize = 56;
use crate::schema::OUTCOMES;
use crate::schema::STATS;
use redb::{Database, Durability, ReadableDatabase, ReadableTable};

const PRUNE_CURSOR_KEY: &[u8] = b"outcome_prune_cursor_v1";
const PRUNE_CURSOR_MAGIC: &[u8; 4] = b"OCS1";
const PRUNE_CURSOR_LEN: usize = 20;
pub const MAX_OUTCOME_PRUNE_BATCH: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeStatus {
    Committed,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationOutcome {
    pub status: OutcomeStatus,
    pub invocation_id: [u8; 16],
    pub principal_id: [u8; 16],
    /// Nonzero for COMMITTED, zero for ABORTED.
    pub commit_sequence: u64,
    pub retention_deadline_us: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutcomeError {
    InvalidLength,
    InvalidMagic,
    UnsupportedVersion,
    InvalidStatus,
    InvalidRetentionState,
    InvalidId,
    InvalidCommitSequence,
    InvalidRetentionDeadline,
}

#[derive(Debug, Eq, PartialEq)]
pub enum OutcomeMaintenanceError {
    InvalidTime,
    InvalidLimit,
    InvalidCursor,
    InvalidOutcome(OutcomeError),
    Storage(String),
}

impl std::fmt::Display for OutcomeMaintenanceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "outcome maintenance failed: {self:?}")
    }
}

impl std::error::Error for OutcomeMaintenanceError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutcomePruneResult {
    pub scanned: usize,
    pub removed: usize,
    pub cycle_complete: bool,
}

pub fn prune_expired_outcomes(
    database: &Database,
    now_us: i64,
    max_scanned: usize,
) -> Result<OutcomePruneResult, OutcomeMaintenanceError> {
    let result = prune_expired_outcomes_unchecked(database, now_us, max_scanned);
    if matches!(&result, Err(OutcomeMaintenanceError::Storage(_))) {
        crate::storage_manager::mark_database_uncertain(database);
    }
    result
}

fn prune_expired_outcomes_unchecked(
    database: &Database,
    now_us: i64,
    max_scanned: usize,
) -> Result<OutcomePruneResult, OutcomeMaintenanceError> {
    if now_us <= 0 {
        return Err(OutcomeMaintenanceError::InvalidTime);
    }
    if max_scanned == 0 || max_scanned > MAX_OUTCOME_PRUNE_BATCH {
        return Err(OutcomeMaintenanceError::InvalidLimit);
    }

    let mut write = database
        .begin_write()
        .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
    write
        .set_durability(Durability::Immediate)
        .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
    write.set_two_phase_commit(true);

    let cursor = {
        let stats = write
            .open_table(STATS)
            .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
        stats
            .get(PRUNE_CURSOR_KEY)
            .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?
            .map(|value| decode_prune_cursor(value.value()))
            .transpose()?
    };

    let mut scanned_entries = Vec::<([u8; 16], bool)>::with_capacity(max_scanned);
    {
        let outcomes = write
            .open_table(OUTCOMES)
            .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
        if let Some(cursor) = cursor {
            for entry in outcomes
                .range::<&[u8]>(cursor.as_slice()..)
                .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?
            {
                let (key, value) =
                    entry.map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
                if key.value() == cursor.as_slice() {
                    continue;
                }
                scanned_entries.push(decode_prune_entry(key.value(), value.value(), now_us)?);
                if scanned_entries.len() == max_scanned {
                    break;
                }
            }
            if scanned_entries.len() < max_scanned {
                for entry in outcomes
                    .range::<&[u8]>(..)
                    .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?
                {
                    let (key, value) = entry
                        .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
                    if key.value() >= cursor.as_slice() {
                        break;
                    }
                    scanned_entries.push(decode_prune_entry(key.value(), value.value(), now_us)?);
                    if scanned_entries.len() == max_scanned {
                        break;
                    }
                }
            }
        } else {
            for entry in outcomes
                .iter()
                .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?
            {
                let (key, value) =
                    entry.map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
                scanned_entries.push(decode_prune_entry(key.value(), value.value(), now_us)?);
                if scanned_entries.len() == max_scanned {
                    break;
                }
            }
        }
    }

    let cycle_complete = scanned_entries.len() < max_scanned;
    let mut removed = 0;
    {
        let mut outcomes = write
            .open_table(OUTCOMES)
            .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
        for (invocation_id, expired) in &scanned_entries {
            if *expired {
                outcomes
                    .remove(invocation_id.as_slice())
                    .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
                removed += 1;
            }
        }
    }
    {
        let mut stats = write
            .open_table(STATS)
            .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
        if cycle_complete {
            stats
                .remove(PRUNE_CURSOR_KEY)
                .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
        } else if let Some((last_id, _)) = scanned_entries.last() {
            let cursor = encode_prune_cursor(last_id);
            stats
                .insert(PRUNE_CURSOR_KEY, cursor.as_slice())
                .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;
        }
    }
    write
        .commit()
        .map_err(|error| OutcomeMaintenanceError::Storage(error.to_string()))?;

    Ok(OutcomePruneResult {
        scanned: scanned_entries.len(),
        removed,
        cycle_complete,
    })
}

fn decode_prune_entry(
    key: &[u8],
    value: &[u8],
    now_us: i64,
) -> Result<([u8; 16], bool), OutcomeMaintenanceError> {
    let invocation_id: [u8; 16] = key
        .try_into()
        .map_err(|_| OutcomeMaintenanceError::InvalidOutcome(OutcomeError::InvalidId))?;
    let outcome =
        InvocationOutcome::decode(value).map_err(OutcomeMaintenanceError::InvalidOutcome)?;
    if outcome.invocation_id != invocation_id {
        return Err(OutcomeMaintenanceError::InvalidOutcome(
            OutcomeError::InvalidId,
        ));
    }
    Ok((invocation_id, now_us >= outcome.retention_deadline_us))
}

fn encode_prune_cursor(invocation_id: &[u8; 16]) -> [u8; PRUNE_CURSOR_LEN] {
    let mut cursor = [0; PRUNE_CURSOR_LEN];
    cursor[..4].copy_from_slice(PRUNE_CURSOR_MAGIC);
    cursor[4..].copy_from_slice(invocation_id);
    cursor
}

fn decode_prune_cursor(bytes: &[u8]) -> Result<[u8; 16], OutcomeMaintenanceError> {
    if bytes.len() != PRUNE_CURSOR_LEN || &bytes[..4] != PRUNE_CURSOR_MAGIC {
        return Err(OutcomeMaintenanceError::InvalidCursor);
    }
    let mut cursor = [0; 16];
    cursor.copy_from_slice(&bytes[4..]);
    if cursor == [0; 16] {
        return Err(OutcomeMaintenanceError::InvalidCursor);
    }
    Ok(cursor)
}

impl std::fmt::Display for OutcomeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid committed outcome: {self:?}")
    }
}

impl std::error::Error for OutcomeError {}

impl InvocationOutcome {
    /// Encode a retained terminal outcome using fixed-width little-endian fields.
    pub fn encode(&self) -> Result<[u8; ENCODED_LEN], OutcomeError> {
        self.validate()?;
        let mut bytes = [0; ENCODED_LEN];
        bytes[..4].copy_from_slice(MAGIC);
        bytes[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes[6] = match self.status {
            OutcomeStatus::Committed => 1,
            OutcomeStatus::Aborted => 2,
        };
        bytes[7] = 1; // retained
        bytes[8..24].copy_from_slice(&self.invocation_id);
        bytes[24..40].copy_from_slice(&self.principal_id);
        bytes[40..48].copy_from_slice(&self.commit_sequence.to_le_bytes());
        bytes[48..56].copy_from_slice(&self.retention_deadline_us.to_le_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, OutcomeError> {
        if bytes.len() != ENCODED_LEN {
            return Err(OutcomeError::InvalidLength);
        }
        if &bytes[..4] != MAGIC {
            return Err(OutcomeError::InvalidMagic);
        }
        if u16::from_le_bytes(bytes[4..6].try_into().expect("fixed range")) != FORMAT_VERSION {
            return Err(OutcomeError::UnsupportedVersion);
        }
        let status = match bytes[6] {
            1 => OutcomeStatus::Committed,
            2 => OutcomeStatus::Aborted,
            _ => return Err(OutcomeError::InvalidStatus),
        };
        if bytes[7] != 1 {
            return Err(OutcomeError::InvalidRetentionState);
        }
        let mut invocation_id = [0; 16];
        invocation_id.copy_from_slice(&bytes[8..24]);
        let mut principal_id = [0; 16];
        principal_id.copy_from_slice(&bytes[24..40]);
        let outcome = Self {
            status,
            invocation_id,
            principal_id,
            commit_sequence: u64::from_le_bytes(bytes[40..48].try_into().expect("fixed range")),
            retention_deadline_us: i64::from_le_bytes(
                bytes[48..56].try_into().expect("fixed range"),
            ),
        };
        outcome.validate()?;
        Ok(outcome)
    }

    fn validate(&self) -> Result<(), OutcomeError> {
        if self.invocation_id == [0; 16] || self.principal_id == [0; 16] {
            return Err(OutcomeError::InvalidId);
        }
        if (self.status == OutcomeStatus::Committed && self.commit_sequence == 0)
            || (self.status == OutcomeStatus::Aborted && self.commit_sequence != 0)
        {
            return Err(OutcomeError::InvalidCommitSequence);
        }
        if self.retention_deadline_us <= 0 {
            return Err(OutcomeError::InvalidRetentionDeadline);
        }
        Ok(())
    }
}

/// Return a retained outcome only to its owning principal; unknown, mismatched and expired
/// outcomes intentionally share the same absence result.
pub fn lookup_outcome(
    database: &Database,
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    now_us: i64,
) -> Result<Option<InvocationOutcome>, Box<dyn std::error::Error>> {
    let transaction = database.begin_read()?;
    let value = transaction
        .open_table(OUTCOMES)?
        .get(&invocation_id[..])?
        .map(|guard| guard.value().to_vec());
    let Some(bytes) = value else {
        return Ok(None);
    };
    let outcome = InvocationOutcome::decode(&bytes)?;
    if outcome.invocation_id != invocation_id {
        return Err(OutcomeError::InvalidId.into());
    }
    if outcome.principal_id != principal_id || now_us >= outcome.retention_deadline_us {
        return Ok(None);
    }
    Ok(Some(outcome))
}
