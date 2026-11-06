//! Credential verifier profiles for database-issued bearer tokens.

use crate::catalog::{CatalogField, CatalogRecord, WireType, encode_envelope, family_schema};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

pub const TOKEN_SECRET_BYTES: usize = 32;
pub const TOKEN_VERIFIER_BYTES: usize = 32;
pub const TOKEN_ALGORITHM_SHA256: u16 = 1;
pub const TOKEN_ALGORITHM_VERSION: u16 = 1;
pub const MAX_TOKEN_LABEL_BYTES: usize = 128;

/// Decoded verifier metadata. The verifier is kept out of `Debug` output.
pub(crate) struct CredentialVerifier {
    pub(crate) user_id: [u8; 16],
    pub(crate) verifier: [u8; TOKEN_VERIFIER_BYTES],
    pub(crate) issued_at_us: i64,
    pub(crate) expires_at_us: i64,
    pub(crate) revoked: bool,
    pub(crate) revoked_at_us: Option<i64>,
    pub(crate) authority_epoch: u64,
    pub(crate) label: Vec<u8>,
}

impl CredentialVerifier {
    pub(crate) fn matches_secret(&self, secret: &[u8; TOKEN_SECRET_BYTES]) -> bool {
        constant_time_equal(&self.verifier, &hash_token_secret(secret))
    }

    pub(crate) fn is_valid_at(&self, now_us: i64) -> bool {
        !self.revoked && self.issued_at_us <= now_us && now_us < self.expires_at_us
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CredentialProfileError {
    InvalidRecord,
}

/// Hash a uniformly random 256-bit bearer token using the versioned profile.
/// This profile is not intended for human-chosen passwords.
pub fn hash_token_secret(secret: &[u8; TOKEN_SECRET_BYTES]) -> [u8; TOKEN_VERIFIER_BYTES] {
    Sha256::digest(secret).into()
}

pub(crate) fn encode_new_verifier_record(
    token_id: [u8; 16],
    user_id: [u8; 16],
    secret: &[u8; TOKEN_SECRET_BYTES],
    issued_at_us: i64,
    expires_at_us: i64,
    label: &str,
) -> Result<Vec<u8>, CredentialProfileError> {
    let mut key = 16u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&token_id);
    let fields = [
        CatalogField {
            tag: 1,
            wire_type: WireType::Id,
            payload: user_id.to_vec(),
        },
        CatalogField {
            tag: 2,
            wire_type: WireType::U16,
            payload: TOKEN_ALGORITHM_SHA256.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 3,
            wire_type: WireType::U16,
            payload: TOKEN_ALGORITHM_VERSION.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 4,
            wire_type: WireType::Bytes,
            payload: hash_token_secret(secret).to_vec(),
        },
        CatalogField {
            tag: 5,
            wire_type: WireType::I64,
            payload: issued_at_us.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 6,
            wire_type: WireType::I64,
            payload: expires_at_us.to_le_bytes().to_vec(),
        },
        CatalogField {
            tag: 7,
            wire_type: WireType::Bool,
            payload: vec![0],
        },
        CatalogField {
            tag: 9,
            wire_type: WireType::Text,
            payload: label.as_bytes().to_vec(),
        },
        CatalogField {
            tag: 10,
            wire_type: WireType::U64,
            payload: 1u64.to_le_bytes().to_vec(),
        },
    ];
    encode_envelope(
        &family_schema(16).ok_or(CredentialProfileError::InvalidRecord)?,
        &key,
        &fields,
    )
    .map_err(|_| CredentialProfileError::InvalidRecord)
}

pub(crate) fn encode_revoked_verifier_record(
    record: &CatalogRecord,
    revoked_at_us: i64,
) -> Result<Vec<u8>, CredentialProfileError> {
    let current = decode_verifier_record(record)?;
    if current.revoked || revoked_at_us < current.issued_at_us {
        return Err(CredentialProfileError::InvalidRecord);
    }
    let next_epoch = current
        .authority_epoch
        .checked_add(1)
        .ok_or(CredentialProfileError::InvalidRecord)?;
    let mut fields = record.fields.clone();
    for field in &mut fields {
        match field.tag {
            7 => field.payload = vec![1],
            8 => field.payload = revoked_at_us.to_le_bytes().to_vec(),
            10 => field.payload = next_epoch.to_le_bytes().to_vec(),
            _ => {}
        }
    }
    if !fields.iter().any(|field| field.tag == 8) {
        let revoked_at = CatalogField {
            tag: 8,
            wire_type: WireType::I64,
            payload: revoked_at_us.to_le_bytes().to_vec(),
        };
        let insertion = fields
            .iter()
            .position(|field| field.tag > 8)
            .unwrap_or(fields.len());
        fields.insert(insertion, revoked_at);
    }
    encode_envelope(
        &family_schema(16).ok_or(CredentialProfileError::InvalidRecord)?,
        &record.logical_key,
        &fields,
    )
    .map_err(|_| CredentialProfileError::InvalidRecord)
}

pub(crate) fn decode_verifier_record(
    record: &CatalogRecord,
) -> Result<CredentialVerifier, CredentialProfileError> {
    if record.family_code != 16
        || record.logical_key.len() != 19
        || record.logical_key[..2] != 16u16.to_be_bytes()
        || record.logical_key[2] != 1
        || record.logical_key[3..] == [0; 16]
    {
        return Err(CredentialProfileError::InvalidRecord);
    }
    let user_id = field(record, 1)?
        .try_into()
        .map_err(|_| CredentialProfileError::InvalidRecord)?;
    if user_id == [0; 16]
        || u16_field(record, 2)? != TOKEN_ALGORITHM_SHA256
        || u16_field(record, 3)? != TOKEN_ALGORITHM_VERSION
    {
        return Err(CredentialProfileError::InvalidRecord);
    }
    let verifier = field(record, 4)?
        .try_into()
        .map_err(|_| CredentialProfileError::InvalidRecord)?;
    let issued_at_us = i64_field(record, 5)?;
    let expires_at_us = i64_field(record, 6)?;
    let revoked = match field(record, 7)? {
        [0] => false,
        [1] => true,
        _ => return Err(CredentialProfileError::InvalidRecord),
    };
    let revoked_at_us = optional_i64_field(record, 8)?;
    let label = field(record, 9)?;
    if label.len() > MAX_TOKEN_LABEL_BYTES
        || std::str::from_utf8(label).is_err()
        || issued_at_us >= expires_at_us
        || u64_field(record, 10)? == 0
        || (revoked && revoked_at_us.is_none())
        || (!revoked && revoked_at_us.is_some())
        || revoked_at_us.is_some_and(|at| at < issued_at_us)
    {
        return Err(CredentialProfileError::InvalidRecord);
    }
    let authority_epoch = u64_field(record, 10)?;
    Ok(CredentialVerifier {
        user_id,
        verifier,
        issued_at_us,
        expires_at_us,
        revoked,
        revoked_at_us,
        authority_epoch,
        label: label.to_vec(),
    })
}

pub(crate) fn now_unix_micros() -> Option<i64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    i64::try_from(elapsed.as_micros()).ok()
}

fn field(record: &CatalogRecord, tag: u16) -> Result<&[u8], CredentialProfileError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| field.payload.as_slice())
        .ok_or(CredentialProfileError::InvalidRecord)
}

fn u16_field(record: &CatalogRecord, tag: u16) -> Result<u16, CredentialProfileError> {
    field(record, tag)?
        .try_into()
        .map(u16::from_le_bytes)
        .map_err(|_| CredentialProfileError::InvalidRecord)
}

fn i64_field(record: &CatalogRecord, tag: u16) -> Result<i64, CredentialProfileError> {
    field(record, tag)?
        .try_into()
        .map(i64::from_le_bytes)
        .map_err(|_| CredentialProfileError::InvalidRecord)
}

fn u64_field(record: &CatalogRecord, tag: u16) -> Result<u64, CredentialProfileError> {
    field(record, tag)?
        .try_into()
        .map(u64::from_le_bytes)
        .map_err(|_| CredentialProfileError::InvalidRecord)
}

fn optional_i64_field(
    record: &CatalogRecord,
    tag: u16,
) -> Result<Option<i64>, CredentialProfileError> {
    record
        .fields
        .iter()
        .find(|field| field.tag == tag)
        .map(|field| {
            field
                .payload
                .as_slice()
                .try_into()
                .map(i64::from_le_bytes)
                .map_err(|_| CredentialProfileError::InvalidRecord)
        })
        .transpose()
}

fn constant_time_equal(
    left: &[u8; TOKEN_VERIFIER_BYTES],
    right: &[u8; TOKEN_VERIFIER_BYTES],
) -> bool {
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (left, right)| difference | (left ^ right))
        == 0
}
