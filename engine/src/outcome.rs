//! Durable terminal invocation outcome value.
const MAGIC: &[u8; 4] = b"OUT1";
const FORMAT_VERSION: u16 = 1;
const ENCODED_LEN: usize = 56;
use crate::schema::OUTCOMES;
use redb::{Database, ReadableDatabase};

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
