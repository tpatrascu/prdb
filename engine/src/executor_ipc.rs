//! Executor IPC operations that terminate at trusted storage primitives.

use crate::authorization::SanitizedCell;
use crate::ipc::{DispatchError, IpcFrame, Operation, ServiceRole, authorize_operation};
use crate::metadata_discovery::{
    AuthorizedRelationDescription, ExecutorRowHandle, ExecutorScanCursor, ResolvedRelationHandle,
    VisibleColumn,
};
use crate::query_abi as generated;
pub use crate::query_abi::{
    EXECUTOR_BATCH_PAYLOAD_VERSION, EXECUTOR_MUTATION_ACKNOWLEDGEMENT_HEADER_BYTES,
    EXECUTOR_MUTATION_ACKNOWLEDGEMENT_STATUS_OFFSET,
    EXECUTOR_MUTATION_ACKNOWLEDGEMENT_VERSION_OFFSET,
    EXECUTOR_NEXT_BATCH_REQUEST_CURSOR_HANDLE_OFFSET, EXECUTOR_NEXT_BATCH_REQUEST_HEADER_BYTES,
    EXECUTOR_NEXT_BATCH_REQUEST_MAX_BYTES_OFFSET, EXECUTOR_NEXT_BATCH_REQUEST_MAX_SCANNED_OFFSET,
    EXECUTOR_NEXT_BATCH_REQUEST_VERSION_OFFSET,
    EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_CURSOR_HANDLE_OFFSET,
    EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_FINISHED_OFFSET,
    EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_HEADER_BYTES,
    EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_ROW_COUNT_OFFSET,
    EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_VERSION_OFFSET,
    EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_FINISHED_OFFSET,
    EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_HEADER_BYTES,
    EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_ROW_COUNT_OFFSET,
    EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_VERSION_OFFSET,
    EXECUTOR_OPEN_SCAN_REQUEST_COLUMN_COUNT_OFFSET, EXECUTOR_OPEN_SCAN_REQUEST_HEADER_BYTES,
    EXECUTOR_OPEN_SCAN_REQUEST_RELATION_HANDLE_OFFSET, EXECUTOR_OPEN_SCAN_REQUEST_VERSION_OFFSET,
    EXECUTOR_OPEN_SCAN_RESPONSE_CURSOR_HANDLE_OFFSET, EXECUTOR_OPEN_SCAN_RESPONSE_HEADER_BYTES,
    EXECUTOR_OPEN_SCAN_RESPONSE_VERSION_OFFSET, EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES,
    EXECUTOR_RESOLVE_RELATION_REQUEST_RELATION_NAME_LENGTH_OFFSET,
    EXECUTOR_RESOLVE_RELATION_REQUEST_SCHEMA_NAME_LENGTH_OFFSET,
    EXECUTOR_RESOLVE_RELATION_REQUEST_VERSION_OFFSET, EXECUTOR_STAGE_DELETE_REQUEST_HEADER_BYTES,
    EXECUTOR_STAGE_DELETE_REQUEST_ROW_HANDLE_OFFSET, EXECUTOR_STAGE_DELETE_REQUEST_VERSION_OFFSET,
    EXECUTOR_STAGE_INSERT_REQUEST_HEADER_BYTES,
    EXECUTOR_STAGE_INSERT_REQUEST_RELATION_HANDLE_OFFSET,
    EXECUTOR_STAGE_INSERT_REQUEST_VALUE_COUNT_OFFSET, EXECUTOR_STAGE_INSERT_REQUEST_VERSION_OFFSET,
    EXECUTOR_STAGE_INSERT_RESPONSE_HEADER_BYTES, EXECUTOR_STAGE_INSERT_RESPONSE_ROW_HANDLE_OFFSET,
    EXECUTOR_STAGE_INSERT_RESPONSE_VERSION_OFFSET, EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES,
    EXECUTOR_STAGE_UPDATE_REQUEST_PATCH_COUNT_OFFSET,
    EXECUTOR_STAGE_UPDATE_REQUEST_ROW_HANDLE_OFFSET, EXECUTOR_STAGE_UPDATE_REQUEST_VERSION_OFFSET,
    RESOLVE_RELATION_PAYLOAD_VERSION,
};
use crate::transaction::{
    ExecutorMetadataConnection, InsertInput, InvocationTransaction, TransactionError,
};
use crate::tuple::{FieldPatch, ScalarType, ScalarValue};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

const RESOLVE_REQUEST_HEADER_BYTES: usize = EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES as usize;
const RESOLVE_RESPONSE_HEADER_BYTES: usize = 3;
const RESOLVE_VERSION_OFFSET: usize = EXECUTOR_RESOLVE_RELATION_REQUEST_VERSION_OFFSET as usize;
const RESOLVE_SCHEMA_LENGTH_OFFSET: usize =
    EXECUTOR_RESOLVE_RELATION_REQUEST_SCHEMA_NAME_LENGTH_OFFSET as usize;
const RESOLVE_RELATION_LENGTH_OFFSET: usize =
    EXECUTOR_RESOLVE_RELATION_REQUEST_RELATION_NAME_LENGTH_OFFSET as usize;
pub const MAX_EXECUTOR_REQUESTS_PER_CONNECTION: usize = 256;
pub const MAX_EXECUTOR_BATCH_CANDIDATES: u16 = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolveRelationRequest {
    pub schema_name: String,
    pub relation_name: String,
}

impl ResolveRelationRequest {
    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        let schema_len = u16::try_from(self.schema_name.len())
            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
        let relation_len = u16::try_from(self.relation_name.len())
            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
        if self.schema_name.is_empty()
            || self.relation_name.is_empty()
            || self.schema_name.len() + self.relation_name.len() + RESOLVE_REQUEST_HEADER_BYTES
                > crate::ipc::MAX_PAYLOAD_BYTES
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut payload = vec![0; RESOLVE_REQUEST_HEADER_BYTES];
        payload[RESOLVE_VERSION_OFFSET..RESOLVE_VERSION_OFFSET + 2]
            .copy_from_slice(&RESOLVE_RELATION_PAYLOAD_VERSION.to_be_bytes());
        payload[RESOLVE_SCHEMA_LENGTH_OFFSET..RESOLVE_SCHEMA_LENGTH_OFFSET + 2]
            .copy_from_slice(&schema_len.to_be_bytes());
        payload[RESOLVE_RELATION_LENGTH_OFFSET..RESOLVE_RELATION_LENGTH_OFFSET + 2]
            .copy_from_slice(&relation_len.to_be_bytes());
        payload.extend_from_slice(self.schema_name.as_bytes());
        payload.extend_from_slice(self.relation_name.as_bytes());
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < RESOLVE_REQUEST_HEADER_BYTES
            || u16::from_be_bytes([
                payload[RESOLVE_VERSION_OFFSET],
                payload[RESOLVE_VERSION_OFFSET + 1],
            ]) != RESOLVE_RELATION_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let schema_len = usize::from(u16::from_be_bytes([
            payload[RESOLVE_SCHEMA_LENGTH_OFFSET],
            payload[RESOLVE_SCHEMA_LENGTH_OFFSET + 1],
        ]));
        let relation_len = usize::from(u16::from_be_bytes([
            payload[RESOLVE_RELATION_LENGTH_OFFSET],
            payload[RESOLVE_RELATION_LENGTH_OFFSET + 1],
        ]));
        let expected_len = RESOLVE_REQUEST_HEADER_BYTES
            .checked_add(schema_len)
            .and_then(|len| len.checked_add(relation_len))
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        if schema_len == 0 || relation_len == 0 || expected_len != payload.len() {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let schema_end = RESOLVE_REQUEST_HEADER_BYTES + schema_len;
        let schema_name = std::str::from_utf8(&payload[RESOLVE_REQUEST_HEADER_BYTES..schema_end])
            .map_err(|_| ExecutorRequestError::InvalidPayload)?
            .to_owned();
        let relation_name = std::str::from_utf8(&payload[schema_end..])
            .map_err(|_| ExecutorRequestError::InvalidPayload)?
            .to_owned();
        Ok(Self {
            schema_name,
            relation_name,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolveRelationResponse {
    Resolved {
        handle: ResolvedRelationHandle,
        description: AuthorizedRelationDescription,
    },
    Unavailable,
}

impl ResolveRelationResponse {
    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&RESOLVE_RELATION_PAYLOAD_VERSION.to_be_bytes());
        match self {
            Self::Resolved {
                handle,
                description,
            } => {
                payload.push(0);
                payload.extend_from_slice(&description.relation_id);
                payload.extend_from_slice(&handle.to_bytes());
                encode_name(&mut payload, &description.name)?;
                let column_count = u16::try_from(description.columns.len())
                    .map_err(|_| ExecutorRequestError::ResponseTooLarge)?;
                payload.extend_from_slice(&column_count.to_be_bytes());
                for column in &description.columns {
                    encode_visible_column(&mut payload, column)?;
                }
            }
            Self::Unavailable => payload.push(1),
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < RESOLVE_RESPONSE_HEADER_BYTES
            || u16::from_be_bytes([payload[0], payload[1]]) != RESOLVE_RELATION_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        match (payload[2], payload.len()) {
            (0, _) => {
                if payload.len() < 40 {
                    return Err(ExecutorRequestError::InvalidPayload);
                }
                let relation_id = payload[3..19]
                    .try_into()
                    .map_err(|_| ExecutorRequestError::InvalidPayload)?;
                let handle = ResolvedRelationHandle::from_bytes(&payload[19..35])
                    .ok_or(ExecutorRequestError::InvalidPayload)?;
                let mut offset = 35;
                let name = decode_name(payload, &mut offset)?;
                let column_count = read_u16(payload, &mut offset)? as usize;
                if column_count > crate::tuple::MAX_COLUMNS {
                    return Err(ExecutorRequestError::InvalidPayload);
                }
                let mut columns = Vec::with_capacity(column_count);
                for _ in 0..column_count {
                    columns.push(decode_visible_column(payload, &mut offset)?);
                }
                if offset != payload.len() {
                    return Err(ExecutorRequestError::InvalidPayload);
                }
                Ok(Self::Resolved {
                    handle,
                    description: AuthorizedRelationDescription {
                        relation_id,
                        name,
                        columns,
                    },
                })
            }
            (1, RESOLVE_RESPONSE_HEADER_BYTES) => Ok(Self::Unavailable),
            _ => Err(ExecutorRequestError::InvalidPayload),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenScanRequest {
    pub relation: ResolvedRelationHandle,
    /// Empty selects the full declared shape. Otherwise IDs are validated
    /// against the resolved relation; results retain ordinal order.
    pub column_ids: Vec<[u8; 16]>,
}

impl OpenScanRequest {
    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        if self.column_ids.len() > crate::tuple::MAX_COLUMNS
            || self.column_ids.contains(&[0; 16])
            || self
                .column_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.column_ids.len()
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let count = u16::try_from(self.column_ids.len())
            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
        let mut payload = vec![0; EXECUTOR_OPEN_SCAN_REQUEST_HEADER_BYTES as usize];
        let version_offset = EXECUTOR_OPEN_SCAN_REQUEST_VERSION_OFFSET as usize;
        payload[version_offset..version_offset + 2]
            .copy_from_slice(&EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes());
        let handle_offset = EXECUTOR_OPEN_SCAN_REQUEST_RELATION_HANDLE_OFFSET as usize;
        payload[handle_offset..handle_offset + 16].copy_from_slice(&self.relation.to_bytes());
        let count_offset = EXECUTOR_OPEN_SCAN_REQUEST_COLUMN_COUNT_OFFSET as usize;
        payload[count_offset..count_offset + 2].copy_from_slice(&count.to_be_bytes());
        for column_id in &self.column_ids {
            payload.extend_from_slice(column_id);
        }
        if payload.len() > crate::ipc::MAX_PAYLOAD_BYTES {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let header_bytes = EXECUTOR_OPEN_SCAN_REQUEST_HEADER_BYTES as usize;
        let version_offset = EXECUTOR_OPEN_SCAN_REQUEST_VERSION_OFFSET as usize;
        if payload.len() < header_bytes
            || u16::from_be_bytes([payload[version_offset], payload[version_offset + 1]])
                != EXECUTOR_BATCH_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let handle_offset = EXECUTOR_OPEN_SCAN_REQUEST_RELATION_HANDLE_OFFSET as usize;
        let relation =
            ResolvedRelationHandle::from_bytes(&payload[handle_offset..handle_offset + 16])
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        let count_offset = EXECUTOR_OPEN_SCAN_REQUEST_COLUMN_COUNT_OFFSET as usize;
        let column_count = usize::from(u16::from_be_bytes([
            payload[count_offset],
            payload[count_offset + 1],
        ]));
        if column_count > crate::tuple::MAX_COLUMNS
            || payload.len() != header_bytes + column_count * 16
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut column_ids = Vec::with_capacity(column_count);
        for chunk in payload[header_bytes..].chunks_exact(16) {
            column_ids.push(
                chunk
                    .try_into()
                    .map_err(|_| ExecutorRequestError::InvalidPayload)?,
            );
        }
        if column_ids.contains(&[0; 16])
            || column_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != column_ids.len()
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self {
            relation,
            column_ids,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OpenScanResponse {
    pub cursor: ExecutorScanCursor,
}

impl OpenScanResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = vec![0; EXECUTOR_OPEN_SCAN_RESPONSE_HEADER_BYTES as usize];
        let version_offset = EXECUTOR_OPEN_SCAN_RESPONSE_VERSION_OFFSET as usize;
        payload[version_offset..version_offset + 2]
            .copy_from_slice(&EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes());
        let cursor_offset = EXECUTOR_OPEN_SCAN_RESPONSE_CURSOR_HANDLE_OFFSET as usize;
        payload[cursor_offset..cursor_offset + 16].copy_from_slice(&self.cursor.to_bytes());
        payload
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let version_offset = EXECUTOR_OPEN_SCAN_RESPONSE_VERSION_OFFSET as usize;
        let cursor_offset = EXECUTOR_OPEN_SCAN_RESPONSE_CURSOR_HANDLE_OFFSET as usize;
        if payload.len() != EXECUTOR_OPEN_SCAN_RESPONSE_HEADER_BYTES as usize
            || u16::from_be_bytes([payload[version_offset], payload[version_offset + 1]])
                != EXECUTOR_BATCH_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self {
            cursor: ExecutorScanCursor::from_bytes(&payload[cursor_offset..cursor_offset + 16])
                .ok_or(ExecutorRequestError::InvalidPayload)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NextBatchRequest {
    pub cursor: ExecutorScanCursor,
    pub max_scanned: u16,
    pub max_bytes: u32,
}

impl NextBatchRequest {
    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        validate_batch_limits(self.max_scanned, self.max_bytes)?;
        let mut payload = vec![0; EXECUTOR_NEXT_BATCH_REQUEST_HEADER_BYTES as usize];
        let version_offset = EXECUTOR_NEXT_BATCH_REQUEST_VERSION_OFFSET as usize;
        payload[version_offset..version_offset + 2]
            .copy_from_slice(&EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes());
        let cursor_offset = EXECUTOR_NEXT_BATCH_REQUEST_CURSOR_HANDLE_OFFSET as usize;
        payload[cursor_offset..cursor_offset + 16].copy_from_slice(&self.cursor.to_bytes());
        let scanned_offset = EXECUTOR_NEXT_BATCH_REQUEST_MAX_SCANNED_OFFSET as usize;
        payload[scanned_offset..scanned_offset + 2]
            .copy_from_slice(&self.max_scanned.to_be_bytes());
        let bytes_offset = EXECUTOR_NEXT_BATCH_REQUEST_MAX_BYTES_OFFSET as usize;
        payload[bytes_offset..bytes_offset + 4].copy_from_slice(&self.max_bytes.to_be_bytes());
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let version_offset = EXECUTOR_NEXT_BATCH_REQUEST_VERSION_OFFSET as usize;
        let cursor_offset = EXECUTOR_NEXT_BATCH_REQUEST_CURSOR_HANDLE_OFFSET as usize;
        let scanned_offset = EXECUTOR_NEXT_BATCH_REQUEST_MAX_SCANNED_OFFSET as usize;
        let bytes_offset = EXECUTOR_NEXT_BATCH_REQUEST_MAX_BYTES_OFFSET as usize;
        if payload.len() != EXECUTOR_NEXT_BATCH_REQUEST_HEADER_BYTES as usize
            || u16::from_be_bytes([payload[version_offset], payload[version_offset + 1]])
                != EXECUTOR_BATCH_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let request = Self {
            cursor: ExecutorScanCursor::from_bytes(&payload[cursor_offset..cursor_offset + 16])
                .ok_or(ExecutorRequestError::InvalidPayload)?,
            max_scanned: u16::from_be_bytes([payload[scanned_offset], payload[scanned_offset + 1]]),
            max_bytes: u32::from_be_bytes(
                payload[bytes_offset..bytes_offset + 4]
                    .try_into()
                    .expect("fixed payload"),
            ),
        };
        validate_batch_limits(request.max_scanned, request.max_bytes)?;
        Ok(request)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CloseCursorRequest {
    pub cursor: ExecutorScanCursor,
}

impl CloseCursorRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = vec![0; EXECUTOR_OPEN_SCAN_RESPONSE_HEADER_BYTES as usize];
        let version_offset = EXECUTOR_OPEN_SCAN_RESPONSE_VERSION_OFFSET as usize;
        payload[version_offset..version_offset + 2]
            .copy_from_slice(&EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes());
        let cursor_offset = EXECUTOR_OPEN_SCAN_RESPONSE_CURSOR_HANDLE_OFFSET as usize;
        payload[cursor_offset..cursor_offset + 16].copy_from_slice(&self.cursor.to_bytes());
        payload
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let version_offset = EXECUTOR_OPEN_SCAN_RESPONSE_VERSION_OFFSET as usize;
        let cursor_offset = EXECUTOR_OPEN_SCAN_RESPONSE_CURSOR_HANDLE_OFFSET as usize;
        if payload.len() != EXECUTOR_OPEN_SCAN_RESPONSE_HEADER_BYTES as usize
            || u16::from_be_bytes([payload[version_offset], payload[version_offset + 1]])
                != EXECUTOR_BATCH_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self {
            cursor: ExecutorScanCursor::from_bytes(&payload[cursor_offset..cursor_offset + 16])
                .ok_or(ExecutorRequestError::InvalidPayload)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageInsertRequest {
    pub relation: ResolvedRelationHandle,
    pub values: Vec<InsertInput>,
}

impl StageInsertRequest {
    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        if self.values.len() > crate::tuple::MAX_COLUMNS {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut payload = vec![0; EXECUTOR_STAGE_INSERT_REQUEST_HEADER_BYTES as usize];
        let version_offset = EXECUTOR_STAGE_INSERT_REQUEST_VERSION_OFFSET as usize;
        payload[version_offset..version_offset + 2]
            .copy_from_slice(&EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes());
        let relation_offset = EXECUTOR_STAGE_INSERT_REQUEST_RELATION_HANDLE_OFFSET as usize;
        payload[relation_offset..relation_offset + 16].copy_from_slice(&self.relation.to_bytes());
        let count_offset = EXECUTOR_STAGE_INSERT_REQUEST_VALUE_COUNT_OFFSET as usize;
        payload[count_offset..count_offset + 2].copy_from_slice(
            &u16::try_from(self.values.len())
                .map_err(|_| ExecutorRequestError::InvalidPayload)?
                .to_be_bytes(),
        );
        for value in &self.values {
            match value {
                InsertInput::Default => payload.push(2),
                InsertInput::Value(None) => payload.push(1),
                InsertInput::Value(Some(value)) => {
                    payload.push(0);
                    let (type_code, bytes) = encode_scalar_value(value)?;
                    payload.push(type_code);
                    let len = u32::try_from(bytes.len())
                        .map_err(|_| ExecutorRequestError::InvalidPayload)?;
                    payload.extend_from_slice(&len.to_be_bytes());
                    payload.extend_from_slice(&bytes);
                }
            }
            ensure_response_size(&payload)?;
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < EXECUTOR_STAGE_INSERT_REQUEST_HEADER_BYTES as usize
            || payload.len() > crate::ipc::MAX_PAYLOAD_BYTES
            || u16::from_be_bytes([
                payload[EXECUTOR_STAGE_INSERT_REQUEST_VERSION_OFFSET as usize],
                payload[EXECUTOR_STAGE_INSERT_REQUEST_VERSION_OFFSET as usize + 1],
            ]) != EXECUTOR_BATCH_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let relation_offset = EXECUTOR_STAGE_INSERT_REQUEST_RELATION_HANDLE_OFFSET as usize;
        let relation =
            ResolvedRelationHandle::from_bytes(&payload[relation_offset..relation_offset + 16])
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        let count_offset = EXECUTOR_STAGE_INSERT_REQUEST_VALUE_COUNT_OFFSET as usize;
        let count = usize::from(u16::from_be_bytes([
            payload[count_offset],
            payload[count_offset + 1],
        ]));
        if count > crate::tuple::MAX_COLUMNS {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut offset = EXECUTOR_STAGE_INSERT_REQUEST_HEADER_BYTES as usize;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let state = *payload
                .get(offset)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            if state == 2 {
                offset += 1;
                values.push(InsertInput::Default);
            } else {
                values.push(InsertInput::Value(decode_mutation_value(
                    payload,
                    &mut offset,
                )?));
            }
        }
        if offset != payload.len() {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self { relation, values })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageInsertResponse {
    pub handle: ExecutorRowHandle,
}

impl StageInsertResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = vec![0; EXECUTOR_STAGE_INSERT_RESPONSE_HEADER_BYTES as usize];
        let version_offset = EXECUTOR_STAGE_INSERT_RESPONSE_VERSION_OFFSET as usize;
        payload[version_offset..version_offset + 2]
            .copy_from_slice(&EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes());
        let handle_offset = EXECUTOR_STAGE_INSERT_RESPONSE_ROW_HANDLE_OFFSET as usize;
        payload[handle_offset..handle_offset + 16].copy_from_slice(&self.handle.to_bytes());
        payload
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let version_offset = EXECUTOR_STAGE_INSERT_RESPONSE_VERSION_OFFSET as usize;
        let handle_offset = EXECUTOR_STAGE_INSERT_RESPONSE_ROW_HANDLE_OFFSET as usize;
        if payload.len() != EXECUTOR_STAGE_INSERT_RESPONSE_HEADER_BYTES as usize
            || u16::from_be_bytes([payload[version_offset], payload[version_offset + 1]])
                != EXECUTOR_BATCH_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self {
            handle: ExecutorRowHandle::from_bytes(&payload[handle_offset..handle_offset + 16])
                .ok_or(ExecutorRequestError::InvalidPayload)?,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageUpdateRequest {
    pub handle: ExecutorRowHandle,
    pub patches: Vec<FieldPatch>,
}

impl StageUpdateRequest {
    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        if self.patches.len() > crate::tuple::MAX_COLUMNS
            || self
                .patches
                .iter()
                .any(|patch| patch.ordinal >= crate::tuple::MAX_COLUMNS)
            || self
                .patches
                .iter()
                .map(|patch| patch.ordinal)
                .collect::<std::collections::HashSet<_>>()
                .len()
                != self.patches.len()
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut payload = vec![0; EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES as usize];
        let version_offset = EXECUTOR_STAGE_UPDATE_REQUEST_VERSION_OFFSET as usize;
        payload[version_offset..version_offset + 2]
            .copy_from_slice(&EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes());
        let handle_offset = EXECUTOR_STAGE_UPDATE_REQUEST_ROW_HANDLE_OFFSET as usize;
        payload[handle_offset..handle_offset + 16].copy_from_slice(&self.handle.to_bytes());
        let count_offset = EXECUTOR_STAGE_UPDATE_REQUEST_PATCH_COUNT_OFFSET as usize;
        payload[count_offset..count_offset + 2].copy_from_slice(
            &u16::try_from(self.patches.len())
                .map_err(|_| ExecutorRequestError::InvalidPayload)?
                .to_be_bytes(),
        );
        for patch in &self.patches {
            payload.extend_from_slice(
                &u32::try_from(patch.ordinal)
                    .map_err(|_| ExecutorRequestError::InvalidPayload)?
                    .to_be_bytes(),
            );
            encode_mutation_value(&mut payload, patch.value.as_ref())?;
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES as usize
            || payload.len() > crate::ipc::MAX_PAYLOAD_BYTES
            || u16::from_be_bytes([
                payload[EXECUTOR_STAGE_UPDATE_REQUEST_VERSION_OFFSET as usize],
                payload[EXECUTOR_STAGE_UPDATE_REQUEST_VERSION_OFFSET as usize + 1],
            ]) != EXECUTOR_BATCH_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let handle_offset = EXECUTOR_STAGE_UPDATE_REQUEST_ROW_HANDLE_OFFSET as usize;
        let handle = ExecutorRowHandle::from_bytes(&payload[handle_offset..handle_offset + 16])
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let count_offset = EXECUTOR_STAGE_UPDATE_REQUEST_PATCH_COUNT_OFFSET as usize;
        let count = usize::from(u16::from_be_bytes([
            payload[count_offset],
            payload[count_offset + 1],
        ]));
        if count > crate::tuple::MAX_COLUMNS {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut offset = EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES as usize;
        let mut patches = Vec::with_capacity(count);
        let mut ordinals = std::collections::HashSet::with_capacity(count);
        for _ in 0..count {
            let ordinal = usize::try_from(read_u32(payload, &mut offset)?)
                .map_err(|_| ExecutorRequestError::InvalidPayload)?;
            if ordinal >= crate::tuple::MAX_COLUMNS || !ordinals.insert(ordinal) {
                return Err(ExecutorRequestError::InvalidPayload);
            }
            patches.push(FieldPatch {
                ordinal,
                value: decode_mutation_value(payload, &mut offset)?,
            });
        }
        if offset != payload.len() {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self { handle, patches })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageDeleteRequest {
    pub handle: ExecutorRowHandle,
}

impl StageDeleteRequest {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = vec![0; EXECUTOR_STAGE_DELETE_REQUEST_HEADER_BYTES as usize];
        let version_offset = EXECUTOR_STAGE_DELETE_REQUEST_VERSION_OFFSET as usize;
        payload[version_offset..version_offset + 2]
            .copy_from_slice(&EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes());
        let handle_offset = EXECUTOR_STAGE_DELETE_REQUEST_ROW_HANDLE_OFFSET as usize;
        payload[handle_offset..handle_offset + 16].copy_from_slice(&self.handle.to_bytes());
        payload
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let version_offset = EXECUTOR_STAGE_DELETE_REQUEST_VERSION_OFFSET as usize;
        let handle_offset = EXECUTOR_STAGE_DELETE_REQUEST_ROW_HANDLE_OFFSET as usize;
        if payload.len() != EXECUTOR_STAGE_DELETE_REQUEST_HEADER_BYTES as usize
            || u16::from_be_bytes([payload[version_offset], payload[version_offset + 1]])
                != EXECUTOR_BATCH_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self {
            handle: ExecutorRowHandle::from_bytes(&payload[handle_offset..handle_offset + 16])
                .ok_or(ExecutorRequestError::InvalidPayload)?,
        })
    }
}

fn encode_mutation_value(
    payload: &mut Vec<u8>,
    value: Option<&ScalarValue>,
) -> Result<(), ExecutorRequestError> {
    match value {
        None => payload.push(1),
        Some(value) => {
            payload.push(0);
            let (type_code, bytes) = encode_scalar_value(value)?;
            payload.push(type_code);
            let len =
                u32::try_from(bytes.len()).map_err(|_| ExecutorRequestError::InvalidPayload)?;
            payload.extend_from_slice(&len.to_be_bytes());
            payload.extend_from_slice(&bytes);
        }
    }
    ensure_response_size(payload)
}

fn decode_mutation_value(
    payload: &[u8],
    offset: &mut usize,
) -> Result<Option<ScalarValue>, ExecutorRequestError> {
    let state = *payload
        .get(*offset)
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    *offset += 1;
    match state {
        1 => Ok(None),
        0 => {
            let type_code = *payload
                .get(*offset)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            *offset += 1;
            let len = usize::try_from(read_u32(payload, offset)?)
                .map_err(|_| ExecutorRequestError::InvalidPayload)?;
            let end = offset
                .checked_add(len)
                .filter(|end| *end <= payload.len())
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            let value = decode_scalar_value(type_code, &payload[*offset..end])?;
            *offset = end;
            Ok(Some(value))
        }
        _ => Err(ExecutorRequestError::InvalidPayload),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationAcknowledgement;

impl MutationAcknowledgement {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = vec![0; EXECUTOR_MUTATION_ACKNOWLEDGEMENT_HEADER_BYTES as usize];
        let version_offset = EXECUTOR_MUTATION_ACKNOWLEDGEMENT_VERSION_OFFSET as usize;
        payload[version_offset..version_offset + 2]
            .copy_from_slice(&EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes());
        payload[EXECUTOR_MUTATION_ACKNOWLEDGEMENT_STATUS_OFFSET as usize] = 0;
        payload
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let version_offset = EXECUTOR_MUTATION_ACKNOWLEDGEMENT_VERSION_OFFSET as usize;
        if payload.len() != EXECUTOR_MUTATION_ACKNOWLEDGEMENT_HEADER_BYTES as usize
            || u16::from_be_bytes([payload[version_offset], payload[version_offset + 1]])
                != EXECUTOR_BATCH_PAYLOAD_VERSION
            || payload[EXECUTOR_MUTATION_ACKNOWLEDGEMENT_STATUS_OFFSET as usize] != 0
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutorBatchRow {
    pub handle: ExecutorRowHandle,
    pub cells: Vec<SanitizedCell>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NextBatchResponse {
    pub rows: Vec<ExecutorBatchRow>,
    pub continuation: Option<ExecutorScanCursor>,
}

impl NextBatchResponse {
    pub fn encode(&self, max_bytes: u32) -> Result<Vec<u8>, ExecutorRequestError> {
        validate_batch_limits(1, max_bytes)?;
        let max_bytes =
            usize::try_from(max_bytes).map_err(|_| ExecutorRequestError::ResponseTooLarge)?;
        let (header_bytes, version_offset, finished_offset, row_count_offset) =
            if self.continuation.is_some() {
                (
                    EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_HEADER_BYTES,
                    EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_VERSION_OFFSET,
                    EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_FINISHED_OFFSET,
                    EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_ROW_COUNT_OFFSET,
                )
            } else {
                (
                    EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_HEADER_BYTES,
                    EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_VERSION_OFFSET,
                    EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_FINISHED_OFFSET,
                    EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_ROW_COUNT_OFFSET,
                )
            };
        let mut payload = vec![0; header_bytes as usize];
        let version_offset = version_offset as usize;
        payload[version_offset..version_offset + 2]
            .copy_from_slice(&EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes());
        payload[finished_offset as usize] = u8::from(self.continuation.is_none());
        if let Some(cursor) = &self.continuation {
            let cursor_offset =
                EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_CURSOR_HANDLE_OFFSET as usize;
            payload[cursor_offset..cursor_offset + 16].copy_from_slice(&cursor.to_bytes());
        }
        let row_count =
            u16::try_from(self.rows.len()).map_err(|_| ExecutorRequestError::ResponseTooLarge)?;
        let row_count_offset = row_count_offset as usize;
        payload[row_count_offset..row_count_offset + 2].copy_from_slice(&row_count.to_be_bytes());
        ensure_batch_size(&payload, max_bytes)?;
        for row in &self.rows {
            payload.extend_from_slice(&row.handle.to_bytes());
            let cell_count = u16::try_from(row.cells.len())
                .map_err(|_| ExecutorRequestError::ResponseTooLarge)?;
            payload.extend_from_slice(&cell_count.to_be_bytes());
            ensure_batch_size(&payload, max_bytes)?;
            for cell in &row.cells {
                encode_sanitized_cell(&mut payload, cell, max_bytes)?;
            }
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() > crate::ipc::MAX_PAYLOAD_BYTES || payload.len() < 5 {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let version_offset = EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_VERSION_OFFSET as usize;
        if u16::from_be_bytes([payload[version_offset], payload[version_offset + 1]])
            != EXECUTOR_BATCH_PAYLOAD_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let finished_offset = EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_FINISHED_OFFSET as usize;
        let finished = payload[finished_offset];
        if finished > 1 {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let continuation = if finished == 0 {
            let header_bytes = EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_HEADER_BYTES as usize;
            let cursor_offset =
                EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_CURSOR_HANDLE_OFFSET as usize;
            if payload.len() < header_bytes {
                return Err(ExecutorRequestError::InvalidPayload);
            }
            Some(
                ExecutorScanCursor::from_bytes(
                    payload[cursor_offset..cursor_offset + 16]
                        .try_into()
                        .map_err(|_| ExecutorRequestError::InvalidPayload)?,
                )
                .ok_or(ExecutorRequestError::InvalidPayload)?,
            )
        } else {
            None
        };
        let (header_bytes, row_count_offset) = if continuation.is_some() {
            (
                EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_HEADER_BYTES as usize,
                EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_ROW_COUNT_OFFSET as usize,
            )
        } else {
            (
                EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_HEADER_BYTES as usize,
                EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_ROW_COUNT_OFFSET as usize,
            )
        };
        let row_count = usize::from(u16::from_be_bytes([
            payload[row_count_offset],
            payload[row_count_offset + 1],
        ]));
        let mut offset = header_bytes;
        if row_count > usize::from(MAX_EXECUTOR_BATCH_CANDIDATES) {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let handle = ExecutorRowHandle::from_bytes(&read_array_16(payload, &mut offset)?)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            let cell_count = usize::from(read_u16(payload, &mut offset)?);
            if cell_count > crate::tuple::MAX_COLUMNS {
                return Err(ExecutorRequestError::InvalidPayload);
            }
            let mut cells = Vec::with_capacity(cell_count);
            for _ in 0..cell_count {
                let state = *payload
                    .get(offset)
                    .ok_or(ExecutorRequestError::InvalidPayload)?;
                offset += 1;
                cells.push(match state {
                    0 => {
                        let type_code = *payload
                            .get(offset)
                            .ok_or(ExecutorRequestError::InvalidPayload)?;
                        offset += 1;
                        let value_len = usize::try_from(read_u32(payload, &mut offset)?)
                            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
                        let value_end = offset
                            .checked_add(value_len)
                            .filter(|end| *end <= payload.len())
                            .ok_or(ExecutorRequestError::InvalidPayload)?;
                        let value = decode_scalar_value(type_code, &payload[offset..value_end])?;
                        offset = value_end;
                        SanitizedCell::Value(value)
                    }
                    1 => SanitizedCell::Null,
                    2 => SanitizedCell::Redacted,
                    _ => return Err(ExecutorRequestError::InvalidPayload),
                });
            }
            rows.push(ExecutorBatchRow { handle, cells });
        }
        if offset != payload.len() {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self { rows, continuation })
    }
}

fn validate_batch_limits(max_scanned: u16, max_bytes: u32) -> Result<(), ExecutorRequestError> {
    if max_scanned == 0
        || max_scanned > MAX_EXECUTOR_BATCH_CANDIDATES
        || max_bytes < 8
        || usize::try_from(max_bytes).map_or(true, |bytes| bytes > crate::ipc::MAX_PAYLOAD_BYTES)
    {
        return Err(ExecutorRequestError::InvalidPayload);
    }
    Ok(())
}

pub(crate) fn ensure_response_size(payload: &[u8]) -> Result<(), ExecutorRequestError> {
    if payload.len() > crate::ipc::MAX_PAYLOAD_BYTES {
        Err(ExecutorRequestError::ResponseTooLarge)
    } else {
        Ok(())
    }
}

fn ensure_batch_size(payload: &[u8], max_bytes: usize) -> Result<(), ExecutorRequestError> {
    if payload.len() > max_bytes || payload.len() > crate::ipc::MAX_PAYLOAD_BYTES {
        Err(ExecutorRequestError::ResponseTooLarge)
    } else {
        Ok(())
    }
}

fn encode_sanitized_cell(
    payload: &mut Vec<u8>,
    cell: &SanitizedCell,
    max_bytes: usize,
) -> Result<(), ExecutorRequestError> {
    match cell {
        SanitizedCell::Null => payload.push(1),
        SanitizedCell::Redacted => payload.push(2),
        SanitizedCell::Value(value) => {
            payload.push(0);
            let (type_code, bytes) = encode_scalar_value(value)?;
            payload.push(type_code);
            let len =
                u32::try_from(bytes.len()).map_err(|_| ExecutorRequestError::ResponseTooLarge)?;
            payload.extend_from_slice(&len.to_be_bytes());
            payload.extend_from_slice(&bytes);
        }
    }
    ensure_batch_size(payload, max_bytes)
}

fn encode_scalar_value(value: &ScalarValue) -> Result<(u8, Vec<u8>), ExecutorRequestError> {
    let encoded = match value {
        ScalarValue::Boolean(value) => (1, vec![u8::from(*value)]),
        ScalarValue::SmallInt(value) => (2, value.to_be_bytes().to_vec()),
        ScalarValue::Integer(value) => (3, value.to_be_bytes().to_vec()),
        ScalarValue::BigInt(value) => (4, value.to_be_bytes().to_vec()),
        ScalarValue::Decimal(value) => (5, value.to_be_bytes().to_vec()),
        ScalarValue::Text(value) => (6, value.as_bytes().to_vec()),
        ScalarValue::Varchar(value) => (7, value.as_bytes().to_vec()),
        ScalarValue::Bytea(value) => (8, value.clone()),
        ScalarValue::Uuid(value) => (9, value.to_vec()),
        ScalarValue::Date(value) => (10, value.to_be_bytes().to_vec()),
        ScalarValue::TimestampTz(value) => (11, value.to_be_bytes().to_vec()),
    };
    Ok(encoded)
}

fn decode_scalar_value(type_code: u8, bytes: &[u8]) -> Result<ScalarValue, ExecutorRequestError> {
    let invalid = || ExecutorRequestError::InvalidPayload;
    let value = match type_code {
        1 if bytes.len() == 1 && bytes[0] <= 1 => ScalarValue::Boolean(bytes[0] == 1),
        2 if bytes.len() == 2 => {
            ScalarValue::SmallInt(i16::from_be_bytes(bytes.try_into().map_err(|_| invalid())?))
        }
        3 if bytes.len() == 4 => {
            ScalarValue::Integer(i32::from_be_bytes(bytes.try_into().map_err(|_| invalid())?))
        }
        4 if bytes.len() == 8 => {
            ScalarValue::BigInt(i64::from_be_bytes(bytes.try_into().map_err(|_| invalid())?))
        }
        5 if bytes.len() == 16 => ScalarValue::Decimal(i128::from_be_bytes(
            bytes.try_into().map_err(|_| invalid())?,
        )),
        6 => ScalarValue::Text(
            std::str::from_utf8(bytes)
                .map_err(|_| invalid())?
                .to_owned(),
        ),
        7 => ScalarValue::Varchar(
            std::str::from_utf8(bytes)
                .map_err(|_| invalid())?
                .to_owned(),
        ),
        8 => ScalarValue::Bytea(bytes.to_vec()),
        9 if bytes.len() == 16 => ScalarValue::Uuid(bytes.try_into().map_err(|_| invalid())?),
        10 if bytes.len() == 4 => {
            ScalarValue::Date(i32::from_be_bytes(bytes.try_into().map_err(|_| invalid())?))
        }
        11 if bytes.len() == 8 => {
            ScalarValue::TimestampTz(i64::from_be_bytes(bytes.try_into().map_err(|_| invalid())?))
        }
        _ => return Err(invalid()),
    };
    Ok(value)
}

pub(crate) fn encode_name(payload: &mut Vec<u8>, name: &str) -> Result<(), ExecutorRequestError> {
    let name_len = u16::try_from(name.len()).map_err(|_| ExecutorRequestError::ResponseTooLarge)?;
    if name.is_empty() {
        return Err(ExecutorRequestError::InvalidPayload);
    }
    payload.extend_from_slice(&name_len.to_be_bytes());
    payload.extend_from_slice(name.as_bytes());
    ensure_response_size(payload)
}

pub(crate) fn decode_name(
    payload: &[u8],
    offset: &mut usize,
) -> Result<String, ExecutorRequestError> {
    let name_len = usize::from(read_u16(payload, offset)?);
    if name_len == 0 {
        return Err(ExecutorRequestError::InvalidPayload);
    }
    let end = offset
        .checked_add(name_len)
        .filter(|end| *end <= payload.len())
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    let name = std::str::from_utf8(&payload[*offset..end])
        .map_err(|_| ExecutorRequestError::InvalidPayload)?
        .to_owned();
    *offset = end;
    Ok(name)
}

pub(crate) fn encode_visible_column(
    payload: &mut Vec<u8>,
    column: &VisibleColumn,
) -> Result<(), ExecutorRequestError> {
    payload.extend_from_slice(&column.column_id);
    payload.extend_from_slice(&column.ordinal.to_be_bytes());
    payload.push(u8::from(column.nullable));
    let (type_code, parameters) = encode_scalar_type(column.scalar_type);
    payload.push(type_code);
    payload.extend_from_slice(&parameters);
    encode_name(payload, &column.name)
}

pub(crate) fn decode_visible_column(
    payload: &[u8],
    offset: &mut usize,
) -> Result<VisibleColumn, ExecutorRequestError> {
    let column_id = read_array_16(payload, offset)?;
    let ordinal = read_u32(payload, offset)?;
    let nullable = *payload
        .get(*offset)
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    *offset += 1;
    let type_code = *payload
        .get(*offset)
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    *offset += 1;
    let param_end = offset
        .checked_add(4)
        .filter(|end| *end <= payload.len())
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    let parameters: [u8; 4] = payload[*offset..param_end]
        .try_into()
        .map_err(|_| ExecutorRequestError::InvalidPayload)?;
    *offset = param_end;
    if nullable > 1 {
        return Err(ExecutorRequestError::InvalidPayload);
    }
    let scalar_type = decode_scalar_type(type_code, parameters)?;
    let name = decode_name(payload, offset)?;
    Ok(VisibleColumn {
        column_id,
        name,
        ordinal,
        scalar_type,
        nullable: nullable == 1,
    })
}

fn encode_scalar_type(scalar_type: ScalarType) -> (u8, [u8; 4]) {
    match scalar_type {
        ScalarType::Boolean => (1, [0; 4]),
        ScalarType::SmallInt => (2, [0; 4]),
        ScalarType::Integer => (3, [0; 4]),
        ScalarType::BigInt => (4, [0; 4]),
        ScalarType::Decimal { precision, scale } => (5, [precision, scale, 0, 0]),
        ScalarType::Text => (6, [0; 4]),
        ScalarType::Varchar { max_chars } => (7, max_chars.to_be_bytes()),
        ScalarType::Bytea => (8, [0; 4]),
        ScalarType::Uuid => (9, [0; 4]),
        ScalarType::Date => (10, [0; 4]),
        ScalarType::TimestampTz => (11, [0; 4]),
    }
}

fn decode_scalar_type(
    type_code: u8,
    parameters: [u8; 4],
) -> Result<ScalarType, ExecutorRequestError> {
    let scalar_type = match type_code {
        1 if parameters == [0; 4] => ScalarType::Boolean,
        2 if parameters == [0; 4] => ScalarType::SmallInt,
        3 if parameters == [0; 4] => ScalarType::Integer,
        4 if parameters == [0; 4] => ScalarType::BigInt,
        5 if parameters[2..] == [0; 2] && parameters[0] != 0 && parameters[1] <= parameters[0] => {
            ScalarType::Decimal {
                precision: parameters[0],
                scale: parameters[1],
            }
        }
        6 if parameters == [0; 4] => ScalarType::Text,
        7 if u32::from_be_bytes(parameters) != 0 => ScalarType::Varchar {
            max_chars: u32::from_be_bytes(parameters),
        },
        8 if parameters == [0; 4] => ScalarType::Bytea,
        9 if parameters == [0; 4] => ScalarType::Uuid,
        10 if parameters == [0; 4] => ScalarType::Date,
        11 if parameters == [0; 4] => ScalarType::TimestampTz,
        _ => return Err(ExecutorRequestError::InvalidPayload),
    };
    Ok(scalar_type)
}

fn read_u16(payload: &[u8], offset: &mut usize) -> Result<u16, ExecutorRequestError> {
    let end = offset
        .checked_add(2)
        .filter(|end| *end <= payload.len())
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    let value = u16::from_be_bytes(
        payload[*offset..end]
            .try_into()
            .map_err(|_| ExecutorRequestError::InvalidPayload)?,
    );
    *offset = end;
    Ok(value)
}

fn read_u32(payload: &[u8], offset: &mut usize) -> Result<u32, ExecutorRequestError> {
    let end = offset
        .checked_add(4)
        .filter(|end| *end <= payload.len())
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    let value = u32::from_be_bytes(
        payload[*offset..end]
            .try_into()
            .map_err(|_| ExecutorRequestError::InvalidPayload)?,
    );
    *offset = end;
    Ok(value)
}

fn read_array_16(payload: &[u8], offset: &mut usize) -> Result<[u8; 16], ExecutorRequestError> {
    let end = offset
        .checked_add(16)
        .filter(|end| *end <= payload.len())
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    let value = payload[*offset..end]
        .try_into()
        .map_err(|_| ExecutorRequestError::InvalidPayload)?;
    *offset = end;
    Ok(value)
}

#[derive(Debug)]
pub enum ExecutorRequestError {
    Dispatch(DispatchError),
    UnsupportedOperation(Operation),
    InvalidPayload,
    ResponseTooLarge,
    Transaction(TransactionError),
}

#[derive(Debug)]
pub enum ExecutorConnectionError {
    Frame(crate::ipc::FrameError),
    Request(ExecutorRequestError),
    DuplicateRequestId,
    RequestLimitExceeded,
    Abort(TransactionError),
    SharedInvocationUnavailable,
    SharedInvocationPoisoned,
}

impl std::fmt::Display for ExecutorRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "executor IPC request failed: {self:?}")
    }
}

impl std::error::Error for ExecutorRequestError {}

impl std::fmt::Display for ExecutorConnectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "executor IPC connection failed: {self:?}")
    }
}

impl std::error::Error for ExecutorConnectionError {}

impl From<TransactionError> for ExecutorRequestError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

pub fn dispatch_executor_resolve_relation(
    role: ServiceRole,
    connection: &mut ExecutorMetadataConnection<'_, '_>,
    request: &IpcFrame,
) -> Result<IpcFrame, ExecutorRequestError> {
    let operation =
        Operation::from_code(request.operation).map_err(ExecutorRequestError::Dispatch)?;
    authorize_operation(role, operation).map_err(ExecutorRequestError::Dispatch)?;
    if operation != Operation::ResolveRelation {
        return Err(ExecutorRequestError::UnsupportedOperation(operation));
    }
    let request_payload = ResolveRelationRequest::decode(&request.payload)?;
    let response = match connection
        .resolve_relation(&request_payload.schema_name, &request_payload.relation_name)?
    {
        Some((handle, description)) => ResolveRelationResponse::Resolved {
            handle,
            description,
        },
        None => ResolveRelationResponse::Unavailable,
    };
    Ok(IpcFrame {
        operation: request.operation,
        request_id: request.request_id,
        payload: response.encode()?,
    })
}

pub fn dispatch_executor_request(
    role: ServiceRole,
    connection: &mut ExecutorMetadataConnection<'_, '_>,
    request: &IpcFrame,
) -> Result<IpcFrame, ExecutorRequestError> {
    let operation =
        Operation::from_code(request.operation).map_err(ExecutorRequestError::Dispatch)?;
    authorize_operation(role, operation).map_err(ExecutorRequestError::Dispatch)?;
    let payload = match operation {
        Operation::ResolveRelation => {
            return dispatch_executor_resolve_relation(role, connection, request);
        }
        Operation::ResolvePlanRelation => {
            let decoded = ResolveRelationRequest::decode(&request.payload)?;
            let relation = connection
                .describe_relation_for_plan(&decoded.schema_name, &decoded.relation_name)?;
            crate::planner_ipc::DescribePlanRelationResponse { relation }
                .encode()
                .map_err(|_| ExecutorRequestError::InvalidPayload)?
        }
        Operation::OpenScan => {
            let decoded = OpenScanRequest::decode(&request.payload)?;
            OpenScanResponse {
                cursor: connection.open_relation_scan(&decoded.relation, &decoded.column_ids)?,
            }
            .encode()
        }
        Operation::NextBatch => {
            let decoded = NextBatchRequest::decode(&request.payload)?;
            let page = connection.next_relation_scan_batch(
                &decoded.cursor,
                usize::from(decoded.max_scanned),
                usize::try_from(decoded.max_bytes)
                    .map_err(|_| ExecutorRequestError::InvalidPayload)?,
            )?;
            let response = NextBatchResponse {
                rows: page
                    .rows
                    .into_iter()
                    .map(|(handle, cells)| ExecutorBatchRow { handle, cells })
                    .collect(),
                continuation: page.continuation,
            };
            response.encode(decoded.max_bytes)?
        }
        Operation::CloseCursor => {
            let decoded = CloseCursorRequest::decode(&request.payload)?;
            connection.close_relation_scan(&decoded.cursor)?;
            let mut payload = EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes().to_vec();
            payload.push(0);
            payload
        }
        Operation::StageInsert => {
            let decoded = StageInsertRequest::decode(&request.payload)?;
            StageInsertResponse {
                handle: connection.stage_insert(&decoded.relation, &decoded.values)?,
            }
            .encode()
        }
        Operation::StageUpdate => {
            let decoded = StageUpdateRequest::decode(&request.payload)?;
            connection.stage_update(&decoded.handle, &decoded.patches)?;
            MutationAcknowledgement.encode()
        }
        Operation::StageDelete => {
            let decoded = StageDeleteRequest::decode(&request.payload)?;
            connection.stage_delete(&decoded.handle)?;
            MutationAcknowledgement.encode()
        }
        Operation::ExecutorCheckCancellation => {
            if !request.payload.is_empty() {
                return Err(ExecutorRequestError::InvalidPayload);
            }
            encode_cancellation_check_response(connection.check_cancellation()?)
        }
        _ => return Err(ExecutorRequestError::UnsupportedOperation(operation)),
    };
    Ok(IpcFrame {
        operation: request.operation,
        request_id: request.request_id,
        payload,
    })
}

fn encode_cancellation_check_response(cancelled: bool) -> Vec<u8> {
    let mut payload =
        vec![0; generated::EXECUTOR_CANCELLATION_CHECK_RESPONSE_HEADER_BYTES as usize];
    let version = generated::EXECUTOR_CANCELLATION_CHECK_RESPONSE_VERSION_OFFSET as usize;
    payload[version..version + 2].copy_from_slice(&EXECUTOR_BATCH_PAYLOAD_VERSION.to_be_bytes());
    payload[generated::EXECUTOR_CANCELLATION_CHECK_RESPONSE_CANCELLED_OFFSET as usize] =
        u8::from(cancelled);
    payload
}

/// Serve executor requests on one provisioned stream. The stream's caller
/// binds it to this invocation out of band. Clean EOF ends the executor
/// session but leaves the decision to commit or abort to the supervisor;
/// framing or request errors abort the invocation.
pub fn serve_executor<S: Read + Write>(
    stream: &mut S,
    transaction: &mut InvocationTransaction<'_>,
) -> Result<(), ExecutorConnectionError> {
    let mut connection = transaction.open_executor_metadata_connection();
    let mut request_ids = std::collections::HashSet::new();
    loop {
        let request = match crate::ipc::read_frame(stream) {
            Ok(Some(request)) => request,
            Ok(None) => {
                drop(connection);
                return Ok(());
            }
            Err(error) => {
                drop(connection);
                return match transaction.abort() {
                    Ok(()) => Err(ExecutorConnectionError::Frame(error)),
                    Err(abort_error) => Err(ExecutorConnectionError::Abort(abort_error)),
                };
            }
        };
        if request_ids.len() >= MAX_EXECUTOR_REQUESTS_PER_CONNECTION {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(ExecutorConnectionError::RequestLimitExceeded),
                Err(abort_error) => Err(ExecutorConnectionError::Abort(abort_error)),
            };
        }
        if !request_ids.insert(request.request_id) {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(ExecutorConnectionError::DuplicateRequestId),
                Err(abort_error) => Err(ExecutorConnectionError::Abort(abort_error)),
            };
        }
        let response =
            match dispatch_executor_request(ServiceRole::Executor, &mut connection, &request) {
                Ok(response) => response,
                Err(error) => {
                    drop(connection);
                    return match transaction.abort() {
                        Ok(()) => Err(ExecutorConnectionError::Request(error)),
                        Err(abort_error) => Err(ExecutorConnectionError::Abort(abort_error)),
                    };
                }
            };
        if let Err(error) = crate::ipc::write_frame(stream, &response) {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(ExecutorConnectionError::Frame(error)),
                Err(abort_error) => Err(ExecutorConnectionError::Abort(abort_error)),
            };
        }
    }
}

/// Serve an executor connection that shares an invocation with planner
/// sessions. The transaction lock is held for one request at a time, while
/// the connection's opaque handles remain private to this stream.
pub fn serve_executor_shared<S: Read + Write>(
    stream: &mut S,
    invocation: &Arc<Mutex<Option<InvocationTransaction<'_>>>>,
) -> Result<(), ExecutorConnectionError> {
    let mut state = crate::transaction::ExecutorMetadataSessionState::default();
    let mut request_ids = std::collections::HashSet::new();
    loop {
        let request = match crate::ipc::read_frame(stream) {
            Ok(Some(request)) => request,
            Ok(None) => {
                finish_shared_executor_session(invocation, &mut state, false)?;
                return Ok(());
            }
            Err(error) => {
                finish_shared_executor_session(invocation, &mut state, true)?;
                return Err(ExecutorConnectionError::Frame(error));
            }
        };
        if request_ids.len() >= MAX_EXECUTOR_REQUESTS_PER_CONNECTION {
            finish_shared_executor_session(invocation, &mut state, true)?;
            return Err(ExecutorConnectionError::RequestLimitExceeded);
        }
        if !request_ids.insert(request.request_id) {
            finish_shared_executor_session(invocation, &mut state, true)?;
            return Err(ExecutorConnectionError::DuplicateRequestId);
        }
        let response = {
            let mut invocation = match invocation.lock() {
                Ok(invocation) => invocation,
                Err(poisoned) => {
                    let mut invocation = poisoned.into_inner();
                    if let Some(mut transaction) = invocation.take() {
                        let _ = transaction.abort();
                    }
                    return Err(ExecutorConnectionError::SharedInvocationPoisoned);
                }
            };
            let transaction = invocation
                .as_mut()
                .ok_or(ExecutorConnectionError::SharedInvocationUnavailable)?;
            let mut connection = transaction
                .open_executor_metadata_connection_with_state(std::mem::take(&mut state));
            let result =
                dispatch_executor_request(ServiceRole::Executor, &mut connection, &request);
            state = connection.detach();
            result
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                finish_shared_executor_session(invocation, &mut state, true)?;
                return Err(ExecutorConnectionError::Request(error));
            }
        };
        if let Err(error) = crate::ipc::write_frame(stream, &response) {
            finish_shared_executor_session(invocation, &mut state, true)?;
            return Err(ExecutorConnectionError::Frame(error));
        }
    }
}

fn finish_shared_executor_session(
    invocation: &Arc<Mutex<Option<InvocationTransaction<'_>>>>,
    state: &mut crate::transaction::ExecutorMetadataSessionState,
    abort: bool,
) -> Result<(), ExecutorConnectionError> {
    let mut invocation = match invocation.lock() {
        Ok(invocation) => invocation,
        Err(poisoned) => {
            let mut invocation = poisoned.into_inner();
            if let Some(mut transaction) = invocation.take() {
                let _ = transaction.abort();
            }
            return Err(ExecutorConnectionError::SharedInvocationPoisoned);
        }
    };
    if let Some(transaction) = invocation.as_mut() {
        let connection =
            transaction.open_executor_metadata_connection_with_state(std::mem::take(state));
        drop(connection);
        if abort {
            transaction
                .abort()
                .map_err(ExecutorConnectionError::Abort)?;
        }
    }
    Ok(())
}

/// Compatibility wrapper for callers of the original resolver-only loop.
pub fn serve_executor_resolve_relation<S: Read + Write>(
    stream: &mut S,
    transaction: &mut InvocationTransaction<'_>,
) -> Result<(), ExecutorConnectionError> {
    serve_executor(stream, transaction)
}

#[cfg(test)]
mod cancellation_response_tests {
    use super::encode_cancellation_check_response;

    #[test]
    fn cancellation_check_response_matches_wire_bytes() {
        assert_eq!(encode_cancellation_check_response(false), [0, 1, 0]);
        assert_eq!(encode_cancellation_check_response(true), [0, 1, 1]);
    }
}
