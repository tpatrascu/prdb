//! Bounded row-value envelope; the schema-aware tuple payload is encoded separately.
const MAGIC: &[u8; 4] = b"RWR1";
const FORMAT_VERSION: u16 = 1;
const HEADER_LEN: usize = 20;
pub const MAX_ROW_RECORD_BYTES: usize = 1_048_576;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowRecord {
    pub row_version: u64,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowRecordError {
    InvalidLength,
    TooLarge,
    InvalidMagic,
    UnsupportedVersion,
    NonzeroFlags,
    InvalidRowVersion,
}

impl std::fmt::Display for RowRecordError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid row record: {self:?}")
    }
}

impl std::error::Error for RowRecordError {}

impl RowRecord {
    pub fn encode(&self) -> Result<Vec<u8>, RowRecordError> {
        if self.row_version == 0 {
            return Err(RowRecordError::InvalidRowVersion);
        }
        let encoded_len = HEADER_LEN
            .checked_add(self.payload.len())
            .ok_or(RowRecordError::TooLarge)?;
        if encoded_len > MAX_ROW_RECORD_BYTES {
            return Err(RowRecordError::TooLarge);
        }
        let payload_len =
            u32::try_from(self.payload.len()).map_err(|_| RowRecordError::TooLarge)?;
        let mut bytes = Vec::with_capacity(encoded_len);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&self.row_version.to_le_bytes());
        bytes.extend_from_slice(&payload_len.to_le_bytes());
        bytes.extend_from_slice(&self.payload);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, RowRecordError> {
        if bytes.len() < HEADER_LEN {
            return Err(RowRecordError::InvalidLength);
        }
        if bytes.len() > MAX_ROW_RECORD_BYTES {
            return Err(RowRecordError::TooLarge);
        }
        if &bytes[..4] != MAGIC {
            return Err(RowRecordError::InvalidMagic);
        }
        if u16::from_le_bytes(bytes[4..6].try_into().expect("fixed range")) != FORMAT_VERSION {
            return Err(RowRecordError::UnsupportedVersion);
        }
        if bytes[6..8] != [0; 2] {
            return Err(RowRecordError::NonzeroFlags);
        }
        let row_version = u64::from_le_bytes(bytes[8..16].try_into().expect("fixed range"));
        if row_version == 0 {
            return Err(RowRecordError::InvalidRowVersion);
        }
        let payload_len =
            u32::from_le_bytes(bytes[16..20].try_into().expect("fixed range")) as usize;
        if HEADER_LEN.checked_add(payload_len) != Some(bytes.len()) {
            return Err(RowRecordError::InvalidLength);
        }
        Ok(Self {
            row_version,
            payload: bytes[HEADER_LEN..].to_vec(),
        })
    }
}
