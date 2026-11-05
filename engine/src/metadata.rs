//! Fixed-width engine format metadata stored in `engine_meta_v1`.
const MAGIC: &[u8; 4] = b"ENG1";
const FORMAT_VERSION: u16 = 1;
const ENCODED_LEN: usize = 46;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Lifecycle {
    Uninitialized = 0,
    Initializing = 1,
    Ready = 2,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EngineMetadata {
    pub database_id: [u8; 16],
    pub catalog_version: u32,
    pub expression_version: u32,
    pub builtin_semantics_version: u32,
    pub commit_sequence: u64,
    pub lifecycle: Lifecycle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataError {
    InvalidLength,
    InvalidMagic,
    UnsupportedVersion,
    InvalidDatabaseId,
    InvalidRequiredVersion,
    InvalidLifecycle,
    NonzeroReserved,
}

impl std::fmt::Display for MetadataError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid engine metadata: {self:?}")
    }
}

impl std::error::Error for MetadataError {}

impl EngineMetadata {
    /// Compatibility of the independent logical database formats supported by this engine.
    pub fn supports_current_formats(&self) -> bool {
        self.catalog_version == 1
            && self.expression_version == 1
            && self.builtin_semantics_version == 1
    }

    /// Encode the canonical 46-byte ENG1 record. Integers use little-endian encoding.
    pub fn encode(&self) -> Result<[u8; ENCODED_LEN], MetadataError> {
        self.validate()?;
        let mut bytes = [0; ENCODED_LEN];
        bytes[..4].copy_from_slice(MAGIC);
        bytes[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        bytes[6..22].copy_from_slice(&self.database_id);
        bytes[22..26].copy_from_slice(&self.catalog_version.to_le_bytes());
        bytes[26..30].copy_from_slice(&self.expression_version.to_le_bytes());
        bytes[30..34].copy_from_slice(&self.builtin_semantics_version.to_le_bytes());
        bytes[34..42].copy_from_slice(&self.commit_sequence.to_le_bytes());
        bytes[42] = self.lifecycle as u8;
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, MetadataError> {
        if bytes.len() != ENCODED_LEN {
            return Err(MetadataError::InvalidLength);
        }
        if &bytes[..4] != MAGIC {
            return Err(MetadataError::InvalidMagic);
        }
        if u16::from_le_bytes(bytes[4..6].try_into().expect("fixed range")) != FORMAT_VERSION {
            return Err(MetadataError::UnsupportedVersion);
        }
        if bytes[43..46] != [0; 3] {
            return Err(MetadataError::NonzeroReserved);
        }
        let lifecycle = match bytes[42] {
            0 => Lifecycle::Uninitialized,
            1 => Lifecycle::Initializing,
            2 => Lifecycle::Ready,
            _ => return Err(MetadataError::InvalidLifecycle),
        };
        let mut database_id = [0; 16];
        database_id.copy_from_slice(&bytes[6..22]);
        let metadata = Self {
            database_id,
            catalog_version: u32::from_le_bytes(bytes[22..26].try_into().expect("fixed range")),
            expression_version: u32::from_le_bytes(bytes[26..30].try_into().expect("fixed range")),
            builtin_semantics_version: u32::from_le_bytes(
                bytes[30..34].try_into().expect("fixed range"),
            ),
            commit_sequence: u64::from_le_bytes(bytes[34..42].try_into().expect("fixed range")),
            lifecycle,
        };
        metadata.validate()?;
        Ok(metadata)
    }

    fn validate(&self) -> Result<(), MetadataError> {
        if self.database_id == [0; 16] {
            return Err(MetadataError::InvalidDatabaseId);
        }
        if self.catalog_version == 0
            || self.expression_version == 0
            || self.builtin_semantics_version == 0
        {
            return Err(MetadataError::InvalidRequiredVersion);
        }
        Ok(())
    }
}
