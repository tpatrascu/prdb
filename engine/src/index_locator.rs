//! Versioned value stored beside a logical secondary-index key.

/// Exact number of bytes in an ILC1 locator.
pub const LOCATOR_LENGTH: usize = 32;

const MAGIC: [u8; 4] = *b"ILC1";
const VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IndexLocator {
    pub row_id: [u8; 16],
    pub row_version: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LocatorError {
    InvalidLength,
    InvalidMagic,
    UnsupportedVersion,
    ReservedBytes,
    InvalidRowVersion,
    InvalidIndexKey,
    RowIdMismatch,
}

impl std::fmt::Display for LocatorError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid index locator: {self:?}")
    }
}

impl std::error::Error for LocatorError {}

impl IndexLocator {
    /// Encode `ILC1`, little-endian format version, zero reserved bytes, row ID,
    /// and the positive row version. The row ID duplicates the key suffix so
    /// reads can detect a mismatched or corrupt locator.
    pub fn encode(self) -> [u8; LOCATOR_LENGTH] {
        let mut bytes = [0; LOCATOR_LENGTH];
        bytes[..4].copy_from_slice(&MAGIC);
        bytes[4..6].copy_from_slice(&VERSION.to_le_bytes());
        bytes[8..24].copy_from_slice(&self.row_id);
        bytes[24..32].copy_from_slice(&self.row_version.to_le_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, LocatorError> {
        if bytes.len() != LOCATOR_LENGTH {
            return Err(LocatorError::InvalidLength);
        }
        if bytes[..4] != MAGIC {
            return Err(LocatorError::InvalidMagic);
        }
        if u16::from_le_bytes([bytes[4], bytes[5]]) != VERSION {
            return Err(LocatorError::UnsupportedVersion);
        }
        if bytes[6..8] != [0, 0] {
            return Err(LocatorError::ReservedBytes);
        }
        let row_version = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        if row_version == 0 {
            return Err(LocatorError::InvalidRowVersion);
        }
        Ok(Self {
            row_id: bytes[8..24].try_into().unwrap(),
            row_version,
        })
    }

    /// Decode a locator and verify that the key's trailing row UUID agrees.
    pub fn decode_for_key(key: &[u8], bytes: &[u8]) -> Result<Self, LocatorError> {
        if key.len() < 32 {
            return Err(LocatorError::InvalidIndexKey);
        }
        let locator = Self::decode(bytes)?;
        if key[key.len() - 16..] != locator.row_id {
            return Err(LocatorError::RowIdMismatch);
        }
        Ok(locator)
    }
}
