//! Bounded, versioned administrative audit event encoding.

const HEADER_LENGTH: usize = 68;
const MAX_TARGETS: usize = 256;
const MAX_EVENT_LENGTH: usize = HEADER_LENGTH + MAX_TARGETS * 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum AuditAction {
    Ddl = 1,
    AuthorityChange = 2,
    TokenLifecycle = 3,
    AuthenticationFailure = 4,
    AdministrativeCancellation = 5,
}

impl TryFrom<u16> for AuditAction {
    type Error = AuditError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Ddl),
            2 => Ok(Self::AuthorityChange),
            3 => Ok(Self::TokenLifecycle),
            4 => Ok(Self::AuthenticationFailure),
            5 => Ok(Self::AdministrativeCancellation),
            _ => Err(AuditError::InvalidAction),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum AuditResult {
    Succeeded = 1,
    Denied = 2,
    Failed = 3,
    Accepted = 4,
}

impl TryFrom<u16> for AuditResult {
    type Error = AuditError;

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Succeeded),
            2 => Ok(Self::Denied),
            3 => Ok(Self::Failed),
            4 => Ok(Self::Accepted),
            _ => Err(AuditError::InvalidResult),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditEvent {
    pub event_id: [u8; 16],
    /// `None` represents a failure before authentication produced a principal.
    pub actor_id: Option<[u8; 16]>,
    pub correlation_id: [u8; 16],
    pub occurred_at_us: i64,
    pub action: AuditAction,
    pub result: AuditResult,
    pub target_ids: Vec<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuditError {
    InvalidLength,
    InvalidMagic,
    UnsupportedVersion,
    InvalidAction,
    InvalidResult,
    InvalidId,
    InvalidTimestamp,
    InvalidTargetList,
}

impl std::fmt::Display for AuditError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid audit event: {self:?}")
    }
}

impl std::error::Error for AuditError {}

impl AuditEvent {
    pub fn encode(&self) -> Result<Vec<u8>, AuditError> {
        self.validate()?;
        let mut bytes = Vec::with_capacity(HEADER_LENGTH + self.target_ids.len() * 16);
        bytes.extend_from_slice(b"AUD1");
        bytes.extend_from_slice(&1u16.to_le_bytes());
        bytes.extend_from_slice(&(self.action as u16).to_le_bytes());
        bytes.extend_from_slice(&(self.result as u16).to_le_bytes());
        bytes.extend_from_slice(&(self.target_ids.len() as u16).to_le_bytes());
        bytes.extend_from_slice(&self.event_id);
        bytes.extend_from_slice(&self.actor_id.unwrap_or([0; 16]));
        bytes.extend_from_slice(&self.correlation_id);
        bytes.extend_from_slice(&self.occurred_at_us.to_le_bytes());
        for target in &self.target_ids {
            bytes.extend_from_slice(target);
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, AuditError> {
        if bytes.len() < HEADER_LENGTH || bytes.len() > MAX_EVENT_LENGTH {
            return Err(AuditError::InvalidLength);
        }
        if &bytes[..4] != b"AUD1" {
            return Err(AuditError::InvalidMagic);
        }
        if u16::from_le_bytes(bytes[4..6].try_into().unwrap()) != 1 {
            return Err(AuditError::UnsupportedVersion);
        }
        let action = AuditAction::try_from(u16::from_le_bytes(bytes[6..8].try_into().unwrap()))?;
        let result = AuditResult::try_from(u16::from_le_bytes(bytes[8..10].try_into().unwrap()))?;
        let target_count = u16::from_le_bytes(bytes[10..12].try_into().unwrap()) as usize;
        if target_count > MAX_TARGETS || bytes.len() != HEADER_LENGTH + target_count * 16 {
            return Err(AuditError::InvalidLength);
        }
        let event_id = bytes[12..28].try_into().unwrap();
        let actor_bytes: [u8; 16] = bytes[28..44].try_into().unwrap();
        let actor_id = (actor_bytes != [0; 16]).then_some(actor_bytes);
        let correlation_id = bytes[44..60].try_into().unwrap();
        let occurred_at_us = i64::from_le_bytes(bytes[60..68].try_into().unwrap());
        let target_ids = bytes[68..]
            .chunks_exact(16)
            .map(|target| target.try_into().unwrap())
            .collect();
        let event = Self {
            event_id,
            actor_id,
            correlation_id,
            occurred_at_us,
            action,
            result,
            target_ids,
        };
        event.validate()?;
        Ok(event)
    }

    fn validate(&self) -> Result<(), AuditError> {
        if self.event_id == [0; 16] || self.correlation_id == [0; 16] {
            return Err(AuditError::InvalidId);
        }
        if self.actor_id == Some([0; 16]) || self.target_ids.contains(&[0; 16]) {
            return Err(AuditError::InvalidId);
        }
        if self.occurred_at_us <= 0 {
            return Err(AuditError::InvalidTimestamp);
        }
        if self.target_ids.len() > MAX_TARGETS
            || self.target_ids.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(AuditError::InvalidTargetList);
        }
        Ok(())
    }
}
