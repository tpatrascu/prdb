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
    EXECUTOR_NEXT_BATCH_REQUEST_VERSION_OFFSET, EXECUTOR_OPEN_SCAN_REQUEST_COLUMN_COUNT_OFFSET,
    EXECUTOR_OPEN_SCAN_REQUEST_HEADER_BYTES, EXECUTOR_OPEN_SCAN_REQUEST_RELATION_HANDLE_OFFSET,
    EXECUTOR_OPEN_SCAN_REQUEST_VERSION_OFFSET, EXECUTOR_OPEN_SCAN_RESPONSE_CURSOR_HANDLE_OFFSET,
    EXECUTOR_OPEN_SCAN_RESPONSE_HEADER_BYTES, EXECUTOR_OPEN_SCAN_RESPONSE_VERSION_OFFSET,
    EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES,
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
};
use crate::transaction::{
    AuthorizedIndexRangeBound, ExecutorIndexRangeScan, ExecutorMetadataConnection,
    ExecutorUniqueIndexProbe, InsertInput, InvocationTransaction, TransactionError,
};
use crate::tuple::{FieldPatch, ScalarType, ScalarValue};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

const RESOLVE_REQUEST_HEADER_BYTES: usize = EXECUTOR_RESOLVE_RELATION_REQUEST_HEADER_BYTES as usize;
const RESOLVE_AVAILABLE_RESPONSE_HEADER_BYTES: usize =
    crate::query_abi::EXECUTOR_RESOLVE_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES as usize;
pub const MAX_EXECUTOR_REQUESTS_PER_CONNECTION: usize = 512;
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
        let mut payload =
            generated::encode_executor_resolve_relation_request(schema_len, relation_len).to_vec();
        let schema_name =
            generated::encode_executor_resolve_relation_request_schema_name(&self.schema_name)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        let relation_name =
            generated::encode_executor_resolve_relation_request_relation_name(&self.relation_name)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        payload.extend_from_slice(&schema_name);
        payload.extend_from_slice(&relation_name);
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let (encoded_schema_len, encoded_relation_len) =
            generated::decode_executor_resolve_relation_request(payload)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        let schema_len = usize::from(encoded_schema_len);
        let relation_len = usize::from(encoded_relation_len);
        let expected_len = RESOLVE_REQUEST_HEADER_BYTES
            .checked_add(schema_len)
            .and_then(|len| len.checked_add(relation_len))
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        if schema_len == 0 || relation_len == 0 || expected_len != payload.len() {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let schema_end = RESOLVE_REQUEST_HEADER_BYTES + schema_len;
        let schema_name = generated::decode_executor_resolve_relation_request_schema_name(
            &payload[RESOLVE_REQUEST_HEADER_BYTES..schema_end],
            encoded_schema_len,
        )
        .ok_or(ExecutorRequestError::InvalidPayload)?
        .to_owned();
        let relation_name = generated::decode_executor_resolve_relation_request_relation_name(
            &payload[schema_end..],
            encoded_relation_len,
        )
        .ok_or(ExecutorRequestError::InvalidPayload)?
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
        let payload = match self {
            Self::Resolved {
                handle,
                description,
            } => {
                if description.name.is_empty() {
                    return Err(ExecutorRequestError::InvalidPayload);
                }
                let mut payload = generated::encode_executor_resolve_relation_available_response(
                    0,
                    description.relation_id,
                    handle.to_bytes(),
                )
                .to_vec();
                let column_count = u16::try_from(description.columns.len())
                    .map_err(|_| ExecutorRequestError::ResponseTooLarge)?;
                let relation_name =
                    generated::encode_executor_resolve_relation_available_response_relation_name(
                        &description.name,
                    )
                    .ok_or(ExecutorRequestError::ResponseTooLarge)?;
                payload.extend_from_slice(&relation_name);
                payload.extend_from_slice(
                    &generated::encode_executor_resolve_relation_available_response_column_count(
                        column_count,
                    ),
                );
                for column in &description.columns {
                    encode_visible_column(&mut payload, column)?;
                }
                payload
            }
            Self::Unavailable => {
                generated::encode_executor_resolve_relation_unavailable_response(1).to_vec()
            }
        };
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if generated::decode_executor_resolve_relation_unavailable_response(payload) == Some(1) {
            return Ok(Self::Unavailable);
        }
        if payload.len() < RESOLVE_AVAILABLE_RESPONSE_HEADER_BYTES {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let (status, relation_id, handle_bytes) =
            generated::decode_executor_resolve_relation_available_response(payload)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        if status != 0 {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let handle = ResolvedRelationHandle::from_bytes(&handle_bytes)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let mut offset = RESOLVE_AVAILABLE_RESPONSE_HEADER_BYTES;
        let (name, name_bytes) =
            generated::decode_executor_resolve_relation_available_response_relation_name(
                payload
                    .get(offset..)
                    .ok_or(ExecutorRequestError::InvalidPayload)?,
            )
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let name = name.to_owned();
        if name.is_empty() {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        offset = offset
            .checked_add(name_bytes)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let column_count = usize::from(
            generated::decode_executor_resolve_relation_available_response_column_count(
                payload
                    .get(offset..)
                    .ok_or(ExecutorRequestError::InvalidPayload)?,
            )
            .ok_or(ExecutorRequestError::InvalidPayload)?,
        );
        offset += 2;
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
        let mut payload =
            generated::encode_executor_open_scan_request(self.relation.to_bytes(), count).to_vec();
        let encoded_column_ids =
            generated::encode_executor_open_scan_request_column_ids(&self.column_ids)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        payload.extend_from_slice(&encoded_column_ids);
        if payload.len() > crate::ipc::MAX_PAYLOAD_BYTES {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let header_bytes = EXECUTOR_OPEN_SCAN_REQUEST_HEADER_BYTES as usize;
        let (relation_bytes, encoded_column_count) =
            generated::decode_executor_open_scan_request(payload)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        let relation = ResolvedRelationHandle::from_bytes(&relation_bytes)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let column_count = usize::from(encoded_column_count);
        if column_count > crate::tuple::MAX_COLUMNS || payload.len() < header_bytes {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let column_ids = generated::decode_executor_open_scan_request_column_ids(
            &payload[header_bytes..],
            encoded_column_count,
        )
        .ok_or(ExecutorRequestError::InvalidPayload)?;
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

/// Bounded request for a catalog-validated unique equality probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProbeUniqueIndexRequest {
    pub relation: ResolvedRelationHandle,
    pub index_id: [u8; 16],
    pub values: Vec<ScalarValue>,
    pub column_ids: Vec<[u8; 16]>,
}

impl ProbeUniqueIndexRequest {
    const HEADER_BYTES: usize =
        generated::EXECUTOR_PROBE_UNIQUE_INDEX_REQUEST_HEADER_BYTES as usize;

    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        if self.index_id == [0; 16]
            || self.values.is_empty()
            || self.values.len() > crate::tuple::MAX_COLUMNS
            || self.column_ids.len() > crate::tuple::MAX_COLUMNS
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
        let key_count =
            u16::try_from(self.values.len()).map_err(|_| ExecutorRequestError::InvalidPayload)?;
        let column_count = u16::try_from(self.column_ids.len())
            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
        let mut payload = generated::encode_executor_probe_unique_index_request(
            self.relation.to_bytes(),
            self.index_id,
            key_count,
            column_count,
        )
        .to_vec();
        for value in &self.values {
            let (type_code, bytes) = encode_scalar_value(value)?;
            append_scalar_value_record(&mut payload, type_code, &bytes)?;
        }
        for column_id in &self.column_ids {
            payload.extend_from_slice(column_id);
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < Self::HEADER_BYTES || payload.len() > crate::ipc::MAX_PAYLOAD_BYTES {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let (relation_handle, index_id, encoded_key_count, encoded_column_count) =
            generated::decode_executor_probe_unique_index_request(payload)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        let relation = ResolvedRelationHandle::from_bytes(&relation_handle)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let key_count = usize::from(encoded_key_count);
        let column_count = usize::from(encoded_column_count);
        if index_id == [0; 16]
            || key_count == 0
            || key_count > crate::tuple::MAX_COLUMNS
            || column_count > crate::tuple::MAX_COLUMNS
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut offset = Self::HEADER_BYTES;
        let mut values = Vec::with_capacity(key_count);
        for _ in 0..key_count {
            values.push(decode_scalar_value_record(payload, &mut offset)?);
        }
        let bytes_len = column_count
            .checked_mul(16)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        if offset.checked_add(bytes_len) != Some(payload.len()) {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut column_ids = Vec::with_capacity(column_count);
        for chunk in payload[offset..].chunks_exact(16) {
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
            index_id,
            values,
            column_ids,
        })
    }
}

/// Bounded request for a leading-key index range scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanIndexRangeRequest {
    pub relation: ResolvedRelationHandle,
    pub index_id: [u8; 16],
    pub lower: Option<AuthorizedIndexRangeBound>,
    pub upper: Option<AuthorizedIndexRangeBound>,
    pub column_ids: Vec<[u8; 16]>,
}

impl ScanIndexRangeRequest {
    const PREFIX_BYTES: usize = generated::EXECUTOR_SCAN_INDEX_RANGE_REQUEST_HEADER_BYTES as usize;
    const HEADER_BYTES: usize = Self::PREFIX_BYTES + 4;

    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        if self.index_id == [0; 16]
            || (self.lower.is_none() && self.upper.is_none())
            || self.column_ids.len() > crate::tuple::MAX_COLUMNS
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
        let column_count = u16::try_from(self.column_ids.len())
            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
        let mut payload = generated::encode_executor_scan_index_range_request(
            self.relation.to_bytes(),
            self.index_id,
        )
        .to_vec();
        for bound in [&self.lower, &self.upper] {
            let flag = match bound {
                None => 0,
                Some(bound) if bound.inclusive => 1,
                Some(_) => 2,
            };
            payload.extend_from_slice(
                &generated::encode_executor_scan_index_range_bound_flag_record(flag),
            );
            if let Some(bound) = bound {
                let (type_code, bytes) = encode_scalar_value(&bound.value)?;
                append_scalar_value_record(&mut payload, type_code, &bytes)?;
            }
        }
        payload.extend_from_slice(
            &generated::encode_executor_scan_index_range_column_count_record(column_count),
        );
        for column_id in &self.column_ids {
            payload.extend_from_slice(column_id);
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < Self::HEADER_BYTES || payload.len() > crate::ipc::MAX_PAYLOAD_BYTES {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let (relation_handle, index_id) =
            generated::decode_executor_scan_index_range_request(payload)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        let relation = ResolvedRelationHandle::from_bytes(&relation_handle)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let mut offset = Self::PREFIX_BYTES;
        let mut decode_bound =
            || -> Result<Option<AuthorizedIndexRangeBound>, ExecutorRequestError> {
                let flag_end = offset
                    .checked_add(
                        generated::EXECUTOR_SCAN_INDEX_RANGE_BOUND_FLAG_RECORD_HEADER_BYTES
                            as usize,
                    )
                    .ok_or(ExecutorRequestError::InvalidPayload)?;
                let flag = generated::decode_executor_scan_index_range_bound_flag_record(
                    payload
                        .get(offset..flag_end)
                        .ok_or(ExecutorRequestError::InvalidPayload)?,
                )
                .ok_or(ExecutorRequestError::InvalidPayload)?;
                offset = flag_end;
                match flag {
                    0 => Ok(None),
                    1 | 2 => {
                        let scalar = decode_scalar_value_record(payload, &mut offset)?;
                        Ok(Some(AuthorizedIndexRangeBound {
                            value: scalar,
                            inclusive: flag == 1,
                        }))
                    }
                    _ => Err(ExecutorRequestError::InvalidPayload),
                }
            };
        let lower = decode_bound()?;
        let upper = decode_bound()?;
        let count_end = offset
            .checked_add(
                generated::EXECUTOR_SCAN_INDEX_RANGE_COLUMN_COUNT_RECORD_HEADER_BYTES as usize,
            )
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let column_count = usize::from(
            generated::decode_executor_scan_index_range_column_count_record(
                payload
                    .get(offset..count_end)
                    .ok_or(ExecutorRequestError::InvalidPayload)?,
            )
            .ok_or(ExecutorRequestError::InvalidPayload)?,
        );
        offset = count_end;
        if index_id == [0; 16]
            || (lower.is_none() && upper.is_none())
            || column_count > crate::tuple::MAX_COLUMNS
            || offset.checked_add(column_count * 16) != Some(payload.len())
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut column_ids = Vec::with_capacity(column_count);
        for chunk in payload[offset..].chunks_exact(16) {
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
            index_id,
            lower,
            upper,
            column_ids,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanIndexPrefixRequest {
    pub relation: ResolvedRelationHandle,
    pub index_id: [u8; 16],
    pub values: Vec<ScalarValue>,
    pub column_ids: Vec<[u8; 16]>,
}

impl ScanIndexPrefixRequest {
    const HEADER_BYTES: usize = 2 + 16 + 16 + 1;

    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        if self.index_id == [0; 16]
            || !(2..=8).contains(&self.values.len())
            || self.column_ids.len() > crate::tuple::MAX_COLUMNS
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
        let mut payload = Vec::new();
        payload.extend_from_slice(&1u16.to_be_bytes());
        payload.extend_from_slice(&self.relation.to_bytes());
        payload.extend_from_slice(&self.index_id);
        payload.push(self.values.len() as u8);
        for value in &self.values {
            let (type_code, bytes) = encode_scalar_value(value)?;
            append_scalar_value_record(&mut payload, type_code, &bytes)?;
        }
        let column_count = u16::try_from(self.column_ids.len())
            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
        payload.extend_from_slice(&column_count.to_be_bytes());
        for column_id in &self.column_ids {
            payload.extend_from_slice(column_id);
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < Self::HEADER_BYTES + 2 || payload.len() > crate::ipc::MAX_PAYLOAD_BYTES {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        if payload.get(..2) != Some(&1u16.to_be_bytes()) {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let relation = ResolvedRelationHandle::from_bytes(
            payload
                .get(2..18)
                .ok_or(ExecutorRequestError::InvalidPayload)?,
        )
        .ok_or(ExecutorRequestError::InvalidPayload)?;
        let index_id: [u8; 16] = payload[18..34]
            .try_into()
            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
        let count = usize::from(payload[34]);
        if index_id == [0; 16] || !(2..=8).contains(&count) {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut offset = Self::HEADER_BYTES;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            values.push(decode_scalar_value_record(payload, &mut offset)?);
        }
        let count_end = offset
            .checked_add(2)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let column_count = usize::from(u16::from_be_bytes(
            payload
                .get(offset..count_end)
                .ok_or(ExecutorRequestError::InvalidPayload)?
                .try_into()
                .map_err(|_| ExecutorRequestError::InvalidPayload)?,
        ));
        offset = count_end;
        if column_count > crate::tuple::MAX_COLUMNS
            || offset.checked_add(column_count * 16) != Some(payload.len())
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let column_ids = payload[offset..]
            .chunks_exact(16)
            .map(|chunk| {
                chunk
                    .try_into()
                    .map_err(|_| ExecutorRequestError::InvalidPayload)
            })
            .collect::<Result<Vec<[u8; 16]>, _>>()?;
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
            index_id,
            values,
            column_ids,
        })
    }
}

/// One leading-key or leading-prefix lookup in a batched index request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanIndexProbe {
    pub values: Vec<ScalarValue>,
}

/// Bounded batch of independent leading-key or leading-prefix index probes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScanIndexRangesRequest {
    pub relation: ResolvedRelationHandle,
    pub index_id: [u8; 16],
    pub probes: Vec<ScanIndexProbe>,
    pub column_ids: Vec<[u8; 16]>,
}

impl ScanIndexRangesRequest {
    const HEADER_BYTES: usize = 2 + 16 + 16 + 2;
    pub const MAX_PROBES: usize = 128;

    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        validate_index_ranges_shape(self.index_id, &self.probes, &self.column_ids)?;
        let mut payload = Vec::with_capacity(Self::HEADER_BYTES);
        payload.extend_from_slice(&1u16.to_be_bytes());
        payload.extend_from_slice(&self.relation.to_bytes());
        payload.extend_from_slice(&self.index_id);
        payload.extend_from_slice(&(self.probes.len() as u16).to_be_bytes());
        for probe in &self.probes {
            payload.push(probe.values.len() as u8);
            for value in &probe.values {
                let (type_code, bytes) = encode_scalar_value(value)?;
                append_scalar_value_record(&mut payload, type_code, &bytes)?;
            }
        }
        payload.extend_from_slice(&(self.column_ids.len() as u16).to_be_bytes());
        for column_id in &self.column_ids {
            payload.extend_from_slice(column_id);
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < Self::HEADER_BYTES + 2
            || payload.len() > crate::ipc::MAX_PAYLOAD_BYTES
            || payload.get(..2) != Some(&1u16.to_be_bytes())
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let relation = ResolvedRelationHandle::from_bytes(
            payload
                .get(2..18)
                .ok_or(ExecutorRequestError::InvalidPayload)?,
        )
        .ok_or(ExecutorRequestError::InvalidPayload)?;
        let index_id = payload
            .get(18..34)
            .ok_or(ExecutorRequestError::InvalidPayload)?
            .try_into()
            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
        let probe_count = usize::from(u16::from_be_bytes(
            payload
                .get(34..36)
                .ok_or(ExecutorRequestError::InvalidPayload)?
                .try_into()
                .map_err(|_| ExecutorRequestError::InvalidPayload)?,
        ));
        if !(1..=Self::MAX_PROBES).contains(&probe_count) {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut offset = Self::HEADER_BYTES;
        let mut probes = Vec::with_capacity(probe_count);
        for _ in 0..probe_count {
            let key_count = usize::from(
                *payload
                    .get(offset)
                    .ok_or(ExecutorRequestError::InvalidPayload)?,
            );
            offset = offset
                .checked_add(1)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            if !(1..=8).contains(&key_count) {
                return Err(ExecutorRequestError::InvalidPayload);
            }
            let mut values = Vec::with_capacity(key_count);
            for _ in 0..key_count {
                values.push(decode_scalar_value_record(payload, &mut offset)?);
            }
            probes.push(ScanIndexProbe { values });
        }
        let count_end = offset
            .checked_add(2)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let column_count = usize::from(u16::from_be_bytes(
            payload
                .get(offset..count_end)
                .ok_or(ExecutorRequestError::InvalidPayload)?
                .try_into()
                .map_err(|_| ExecutorRequestError::InvalidPayload)?,
        ));
        offset = count_end;
        if column_count > crate::tuple::MAX_COLUMNS
            || offset.checked_add(column_count * 16) != Some(payload.len())
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let column_ids = payload[offset..]
            .chunks_exact(16)
            .map(|chunk| {
                chunk
                    .try_into()
                    .map_err(|_| ExecutorRequestError::InvalidPayload)
            })
            .collect::<Result<Vec<[u8; 16]>, _>>()?;
        validate_index_ranges_shape(index_id, &probes, &column_ids)?;
        Ok(Self {
            relation,
            index_id,
            probes,
            column_ids,
        })
    }
}

fn validate_index_ranges_shape(
    index_id: [u8; 16],
    probes: &[ScanIndexProbe],
    column_ids: &[[u8; 16]],
) -> Result<(), ExecutorRequestError> {
    if index_id == [0; 16]
        || !(1..=ScanIndexRangesRequest::MAX_PROBES).contains(&probes.len())
        || probes
            .iter()
            .any(|probe| !(1..=8).contains(&probe.values.len()))
        || column_ids.len() > crate::tuple::MAX_COLUMNS
        || column_ids.contains(&[0; 16])
        || column_ids
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len()
            != column_ids.len()
    {
        return Err(ExecutorRequestError::InvalidPayload);
    }
    Ok(())
}

/// Results are kept in input-probe order. An unavailable result has no groups.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScanIndexRangesResponse {
    Unavailable,
    Groups(Vec<Vec<Vec<SanitizedCell>>>),
}

impl ScanIndexRangesResponse {
    const VERSION: u8 = 1;
    const MAX_GROUP_ROWS: usize = 1_024;
    const MAX_CANDIDATES: usize = 16_384;

    fn unavailable_payload() -> Vec<u8> {
        vec![Self::VERSION, 0]
    }

    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        let Self::Groups(groups) = self else {
            return Ok(Self::unavailable_payload());
        };
        if !(1..=ScanIndexRangesRequest::MAX_PROBES).contains(&groups.len()) {
            return Ok(Self::unavailable_payload());
        }
        let mut candidate_count = 0usize;
        let mut payload = vec![Self::VERSION, 1];
        payload.extend_from_slice(&(groups.len() as u16).to_be_bytes());
        for rows in groups {
            if rows.len() > Self::MAX_GROUP_ROWS {
                return Ok(Self::unavailable_payload());
            }
            candidate_count = candidate_count.saturating_add(rows.len());
            if candidate_count > Self::MAX_CANDIDATES {
                return Ok(Self::unavailable_payload());
            }
            payload.extend_from_slice(&(rows.len() as u16).to_be_bytes());
            for row in rows {
                if row.len() > crate::tuple::MAX_COLUMNS {
                    return Err(ExecutorRequestError::InvalidPayload);
                }
                payload.extend_from_slice(&(row.len() as u16).to_be_bytes());
                for cell in row {
                    if encode_sanitized_cell(&mut payload, cell, crate::ipc::MAX_PAYLOAD_BYTES)
                        .is_err()
                    {
                        return Ok(Self::unavailable_payload());
                    }
                }
            }
        }
        if payload.len() > crate::ipc::MAX_PAYLOAD_BYTES {
            Ok(Self::unavailable_payload())
        } else {
            Ok(payload)
        }
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < 2
            || payload.len() > crate::ipc::MAX_PAYLOAD_BYTES
            || payload[0] != Self::VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        if payload[1] == 0 && payload.len() == 2 {
            return Ok(Self::Unavailable);
        }
        if payload[1] != 1 || payload.len() < 4 {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let group_count = usize::from(u16::from_be_bytes([payload[2], payload[3]]));
        if !(1..=ScanIndexRangesRequest::MAX_PROBES).contains(&group_count) {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut offset = 4usize;
        let mut candidate_count = 0usize;
        let mut groups = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            let count_end = offset
                .checked_add(2)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            let row_count = usize::from(u16::from_be_bytes(
                payload
                    .get(offset..count_end)
                    .ok_or(ExecutorRequestError::InvalidPayload)?
                    .try_into()
                    .map_err(|_| ExecutorRequestError::InvalidPayload)?,
            ));
            offset = count_end;
            candidate_count = candidate_count.saturating_add(row_count);
            if row_count > Self::MAX_GROUP_ROWS || candidate_count > Self::MAX_CANDIDATES {
                return Err(ExecutorRequestError::InvalidPayload);
            }
            let mut rows = Vec::with_capacity(row_count);
            for _ in 0..row_count {
                let count_end = offset
                    .checked_add(2)
                    .ok_or(ExecutorRequestError::InvalidPayload)?;
                let cell_count = usize::from(u16::from_be_bytes(
                    payload
                        .get(offset..count_end)
                        .ok_or(ExecutorRequestError::InvalidPayload)?
                        .try_into()
                        .map_err(|_| ExecutorRequestError::InvalidPayload)?,
                ));
                offset = count_end;
                if cell_count > crate::tuple::MAX_COLUMNS {
                    return Err(ExecutorRequestError::InvalidPayload);
                }
                let mut cells = Vec::with_capacity(cell_count);
                for _ in 0..cell_count {
                    let tag_end = offset
                        .checked_add(generated::SANITIZED_CELL_TAG_RECORD_HEADER_BYTES as usize)
                        .ok_or(ExecutorRequestError::InvalidPayload)?;
                    let tag = generated::decode_sanitized_cell_tag_record(
                        payload
                            .get(offset..tag_end)
                            .ok_or(ExecutorRequestError::InvalidPayload)?,
                    )
                    .ok_or(ExecutorRequestError::InvalidPayload)?;
                    offset = tag_end;
                    cells.push(match tag {
                        0 => {
                            SanitizedCell::Value(decode_scalar_value_record(payload, &mut offset)?)
                        }
                        1 => SanitizedCell::Null,
                        2 => SanitizedCell::Redacted,
                        _ => return Err(ExecutorRequestError::InvalidPayload),
                    });
                }
                rows.push(cells);
            }
            groups.push(rows);
        }
        if offset != payload.len() {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self::Groups(groups))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScanIndexRangeResponse {
    Unavailable,
    Rows(Vec<Vec<SanitizedCell>>),
}

impl ScanIndexRangeResponse {
    const MAX_ROWS: usize = 1_024;

    fn unavailable_payload() -> Vec<u8> {
        generated::encode_index_range_response_unavailable_record(
            generated::INDEX_RANGE_RESPONSE_VERSION,
            0,
        )
        .to_vec()
    }

    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        let Self::Rows(rows) = self else {
            return Ok(Self::unavailable_payload());
        };
        if rows.len() > Self::MAX_ROWS {
            return Ok(Self::unavailable_payload());
        }
        let row_count =
            u16::try_from(rows.len()).map_err(|_| ExecutorRequestError::InvalidPayload)?;
        let mut payload = generated::encode_index_range_response_rows_record(
            generated::INDEX_RANGE_RESPONSE_VERSION,
            1,
            row_count,
        )
        .to_vec();
        for row in rows {
            if row.len() > crate::tuple::MAX_COLUMNS {
                return Err(ExecutorRequestError::InvalidPayload);
            }
            let count =
                u16::try_from(row.len()).map_err(|_| ExecutorRequestError::InvalidPayload)?;
            payload.extend_from_slice(&generated::encode_index_range_response_row_record(count));
            for cell in row {
                if encode_sanitized_cell(&mut payload, cell, crate::ipc::MAX_PAYLOAD_BYTES).is_err()
                {
                    return Ok(Self::unavailable_payload());
                }
            }
        }
        if payload.len() > crate::ipc::MAX_PAYLOAD_BYTES {
            Ok(Self::unavailable_payload())
        } else {
            Ok(payload)
        }
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < 2
            || payload.len() > crate::ipc::MAX_PAYLOAD_BYTES
            || payload[0] != generated::INDEX_RANGE_RESPONSE_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        if generated::decode_index_range_response_unavailable_record(payload)
            == Some((generated::INDEX_RANGE_RESPONSE_VERSION, 0))
        {
            return Ok(Self::Unavailable);
        }
        let (version, status, row_count) =
            generated::decode_index_range_response_rows_record(payload)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        if version != generated::INDEX_RANGE_RESPONSE_VERSION || status != 1 {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let row_count = usize::from(row_count);
        if row_count > Self::MAX_ROWS {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut offset: usize = generated::INDEX_RANGE_RESPONSE_ROWS_RECORD_HEADER_BYTES as usize;
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let count_end = offset
                .checked_add(generated::INDEX_RANGE_RESPONSE_ROW_RECORD_HEADER_BYTES as usize)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            let cell_count = usize::from(
                generated::decode_index_range_response_row_record(
                    payload
                        .get(offset..count_end)
                        .ok_or(ExecutorRequestError::InvalidPayload)?,
                )
                .ok_or(ExecutorRequestError::InvalidPayload)?,
            );
            offset = count_end;
            if cell_count > crate::tuple::MAX_COLUMNS {
                return Err(ExecutorRequestError::InvalidPayload);
            }
            let mut cells = Vec::with_capacity(cell_count);
            for _ in 0..cell_count {
                let state = generated::decode_sanitized_cell_tag_record(
                    payload
                        .get(
                            offset
                                ..offset
                                    + generated::SANITIZED_CELL_TAG_RECORD_HEADER_BYTES as usize,
                        )
                        .ok_or(ExecutorRequestError::InvalidPayload)?,
                )
                .ok_or(ExecutorRequestError::InvalidPayload)?;
                offset += generated::SANITIZED_CELL_TAG_RECORD_HEADER_BYTES as usize;
                cells.push(match state {
                    1 => SanitizedCell::Null,
                    2 => SanitizedCell::Redacted,
                    0 => SanitizedCell::Value(decode_scalar_value_record(payload, &mut offset)?),
                    _ => return Err(ExecutorRequestError::InvalidPayload),
                });
            }
            rows.push(cells);
        }
        if offset != payload.len() {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self::Rows(rows))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProbeUniqueIndexResponse {
    Unavailable,
    NoMatch,
    Match(ExecutorBatchRow),
}

impl ProbeUniqueIndexResponse {
    pub fn encode(&self) -> Result<Vec<u8>, ExecutorRequestError> {
        let mut payload = match self {
            Self::Unavailable => {
                generated::encode_executor_probe_unique_index_response_unavailable_record(
                    generated::PROBE_UNIQUE_INDEX_RESPONSE_VERSION,
                    0,
                )
                .to_vec()
            }
            Self::NoMatch => {
                generated::encode_executor_probe_unique_index_response_no_match_record(
                    generated::PROBE_UNIQUE_INDEX_RESPONSE_VERSION,
                    1,
                )
                .to_vec()
            }
            Self::Match(_) => generated::encode_executor_probe_unique_index_response_match_record(
                generated::PROBE_UNIQUE_INDEX_RESPONSE_VERSION,
                2,
            )
            .to_vec(),
        };
        if let Self::Match(row) = self {
            let count =
                u16::try_from(row.cells.len()).map_err(|_| ExecutorRequestError::InvalidPayload)?;
            payload.extend_from_slice(&generated::encode_executor_batch_row_record(
                row.handle.to_bytes(),
                count,
            ));
            for cell in &row.cells {
                encode_sanitized_cell(&mut payload, cell, crate::ipc::MAX_PAYLOAD_BYTES)?;
            }
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() < 2
            || payload.len() > crate::ipc::MAX_PAYLOAD_BYTES
            || payload[0] != generated::PROBE_UNIQUE_INDEX_RESPONSE_VERSION
        {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        if generated::decode_executor_probe_unique_index_response_unavailable_record(payload)
            == Some((generated::PROBE_UNIQUE_INDEX_RESPONSE_VERSION, 0))
        {
            return Ok(Self::Unavailable);
        }
        if generated::decode_executor_probe_unique_index_response_no_match_record(payload)
            == Some((generated::PROBE_UNIQUE_INDEX_RESPONSE_VERSION, 1))
        {
            return Ok(Self::NoMatch);
        }
        let (version, status) =
            generated::decode_executor_probe_unique_index_response_match_record(payload)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        if version != generated::PROBE_UNIQUE_INDEX_RESPONSE_VERSION || status != 2 {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let body = &payload[2..];
        let (handle_bytes, encoded_count) = generated::decode_executor_batch_row_record(body)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let handle = ExecutorRowHandle::from_bytes(&handle_bytes)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let count = usize::from(encoded_count);
        if count > crate::tuple::MAX_COLUMNS {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut offset = generated::EXECUTOR_BATCH_ROW_RECORD_HEADER_BYTES as usize;
        let mut cells = Vec::with_capacity(count);
        for _ in 0..count {
            let header = body
                .get(offset..)
                .and_then(|bytes| {
                    bytes.get(..generated::SANITIZED_CELL_TAG_RECORD_HEADER_BYTES as usize)
                })
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            let state = generated::decode_sanitized_cell_tag_record(header)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            offset += generated::SANITIZED_CELL_TAG_RECORD_HEADER_BYTES as usize;
            cells.push(match state {
                0 => SanitizedCell::Value(decode_scalar_value_record(body, &mut offset)?),
                1 => SanitizedCell::Null,
                2 => SanitizedCell::Redacted,
                _ => return Err(ExecutorRequestError::InvalidPayload),
            });
        }
        if offset != body.len() {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        Ok(Self::Match(ExecutorBatchRow { handle, cells }))
    }
}

impl OpenScanResponse {
    pub fn encode(&self) -> Vec<u8> {
        generated::encode_executor_open_scan_response(self.cursor.to_bytes()).to_vec()
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let cursor = generated::decode_executor_open_scan_response(payload)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        Ok(Self {
            cursor: ExecutorScanCursor::from_bytes(&cursor)
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
        Ok(generated::encode_executor_next_batch_request(
            self.cursor.to_bytes(),
            self.max_scanned,
            self.max_bytes,
        )
        .to_vec())
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let (cursor, max_scanned, max_bytes) =
            generated::decode_executor_next_batch_request(payload)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        let request = Self {
            cursor: ExecutorScanCursor::from_bytes(&cursor)
                .ok_or(ExecutorRequestError::InvalidPayload)?,
            max_scanned,
            max_bytes,
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
        generated::encode_executor_close_cursor_request(self.cursor.to_bytes()).to_vec()
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let cursor = generated::decode_executor_close_cursor_request(payload)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        Ok(Self {
            cursor: ExecutorScanCursor::from_bytes(&cursor)
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
        let value_count =
            u16::try_from(self.values.len()).map_err(|_| ExecutorRequestError::InvalidPayload)?;
        let mut payload =
            generated::encode_executor_stage_insert_request(self.relation.to_bytes(), value_count)
                .to_vec();
        for value in &self.values {
            match value {
                InsertInput::Default => {
                    payload.extend_from_slice(&generated::encode_mutation_cell_tag_record(2))
                }
                InsertInput::Value(None) => {
                    payload.extend_from_slice(&generated::encode_mutation_cell_tag_record(1))
                }
                InsertInput::Value(Some(value)) => {
                    payload.extend_from_slice(&generated::encode_mutation_cell_tag_record(0));
                    let (type_code, bytes) = encode_scalar_value(value)?;
                    append_scalar_value_record(&mut payload, type_code, &bytes)?;
                }
            }
            ensure_response_size(&payload)?;
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() > crate::ipc::MAX_PAYLOAD_BYTES {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let (relation_bytes, encoded_value_count) =
            generated::decode_executor_stage_insert_request(payload)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        let relation = ResolvedRelationHandle::from_bytes(&relation_bytes)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let count = usize::from(encoded_value_count);
        if count > crate::tuple::MAX_COLUMNS {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut offset = EXECUTOR_STAGE_INSERT_REQUEST_HEADER_BYTES as usize;
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let state = decode_mutation_cell_tag(payload, &mut offset)?;
            match state {
                2 => values.push(InsertInput::Default),
                1 => values.push(InsertInput::Value(None)),
                0 => values.push(InsertInput::Value(Some(decode_scalar_value_record(
                    payload,
                    &mut offset,
                )?))),
                _ => return Err(ExecutorRequestError::InvalidPayload),
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
        generated::encode_executor_stage_insert_response(self.handle.to_bytes()).to_vec()
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let handle = generated::decode_executor_stage_insert_response(payload)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        Ok(Self {
            handle: ExecutorRowHandle::from_bytes(&handle)
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
        let patch_count =
            u16::try_from(self.patches.len()).map_err(|_| ExecutorRequestError::InvalidPayload)?;
        let mut payload =
            generated::encode_executor_stage_update_request(self.handle.to_bytes(), patch_count)
                .to_vec();
        for patch in &self.patches {
            payload.extend_from_slice(&generated::encode_patch_ordinal_record(
                u32::try_from(patch.ordinal).map_err(|_| ExecutorRequestError::InvalidPayload)?,
            ));
            encode_mutation_value(&mut payload, patch.value.as_ref())?;
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if payload.len() > crate::ipc::MAX_PAYLOAD_BYTES {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let (handle_bytes, encoded_patch_count) =
            generated::decode_executor_stage_update_request(payload)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        let handle = ExecutorRowHandle::from_bytes(&handle_bytes)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        let count = usize::from(encoded_patch_count);
        if count > crate::tuple::MAX_COLUMNS {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut offset = EXECUTOR_STAGE_UPDATE_REQUEST_HEADER_BYTES as usize;
        let mut patches = Vec::with_capacity(count);
        let mut ordinals = std::collections::HashSet::with_capacity(count);
        for _ in 0..count {
            let ordinal_bytes = payload
                .get(offset..)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            let ordinal = usize::try_from(
                generated::decode_patch_ordinal_record(
                    ordinal_bytes
                        .get(..generated::PATCH_ORDINAL_RECORD_HEADER_BYTES as usize)
                        .ok_or(ExecutorRequestError::InvalidPayload)?,
                )
                .ok_or(ExecutorRequestError::InvalidPayload)?,
            )
            .map_err(|_| ExecutorRequestError::InvalidPayload)?;
            offset = offset
                .checked_add(generated::PATCH_ORDINAL_RECORD_HEADER_BYTES as usize)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
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
        generated::encode_executor_stage_delete_request(self.handle.to_bytes()).to_vec()
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        let handle = generated::decode_executor_stage_delete_request(payload)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
        Ok(Self {
            handle: ExecutorRowHandle::from_bytes(&handle)
                .ok_or(ExecutorRequestError::InvalidPayload)?,
        })
    }
}

fn encode_mutation_value(
    payload: &mut Vec<u8>,
    value: Option<&ScalarValue>,
) -> Result<(), ExecutorRequestError> {
    match value {
        None => payload.extend_from_slice(&generated::encode_mutation_cell_tag_record(1)),
        Some(value) => {
            payload.extend_from_slice(&generated::encode_mutation_cell_tag_record(0));
            let (type_code, bytes) = encode_scalar_value(value)?;
            append_scalar_value_record(payload, type_code, &bytes)?;
        }
    }
    ensure_response_size(payload)
}

fn decode_mutation_value(
    payload: &[u8],
    offset: &mut usize,
) -> Result<Option<ScalarValue>, ExecutorRequestError> {
    let state = decode_mutation_cell_tag(payload, offset)?;
    match state {
        1 => Ok(None),
        0 => decode_scalar_value_record(payload, offset).map(Some),
        _ => Err(ExecutorRequestError::InvalidPayload),
    }
}

fn decode_mutation_cell_tag(
    payload: &[u8],
    offset: &mut usize,
) -> Result<u8, ExecutorRequestError> {
    let state_bytes = payload
        .get(*offset..)
        .and_then(|bytes| bytes.get(..generated::MUTATION_CELL_TAG_RECORD_HEADER_BYTES as usize))
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    let state = generated::decode_mutation_cell_tag_record(state_bytes)
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    *offset = offset
        .checked_add(generated::MUTATION_CELL_TAG_RECORD_HEADER_BYTES as usize)
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    Ok(state)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MutationAcknowledgement;

impl MutationAcknowledgement {
    pub fn encode(&self) -> Vec<u8> {
        generated::encode_executor_mutation_acknowledgement(0).to_vec()
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ExecutorRequestError> {
        if generated::decode_executor_mutation_acknowledgement(payload) != Some(0) {
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
        let row_count =
            u16::try_from(self.rows.len()).map_err(|_| ExecutorRequestError::ResponseTooLarge)?;
        let mut payload = match &self.continuation {
            Some(cursor) => generated::encode_executor_next_batch_response_continuation(
                0,
                cursor.to_bytes(),
                row_count,
            )
            .to_vec(),
            None => generated::encode_executor_next_batch_response_finished(1, row_count).to_vec(),
        };
        ensure_batch_size(&payload, max_bytes)?;
        for row in &self.rows {
            let cell_count = u16::try_from(row.cells.len())
                .map_err(|_| ExecutorRequestError::ResponseTooLarge)?;
            payload.extend_from_slice(&generated::encode_executor_batch_row_record(
                row.handle.to_bytes(),
                cell_count,
            ));
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
        let (finished, finished_row_count) =
            generated::decode_executor_next_batch_response_finished(payload)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
        if finished > 1 {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let (continuation, header_bytes, row_count) = if finished == 0 {
            let (continued_finished, cursor, row_count) =
                generated::decode_executor_next_batch_response_continuation(payload)
                    .ok_or(ExecutorRequestError::InvalidPayload)?;
            if continued_finished != 0 {
                return Err(ExecutorRequestError::InvalidPayload);
            }
            (
                Some(
                    ExecutorScanCursor::from_bytes(&cursor)
                        .ok_or(ExecutorRequestError::InvalidPayload)?,
                ),
                generated::EXECUTOR_NEXT_BATCH_RESPONSE_CONTINUATION_HEADER_BYTES as usize,
                row_count,
            )
        } else {
            (
                None,
                generated::EXECUTOR_NEXT_BATCH_RESPONSE_FINISHED_HEADER_BYTES as usize,
                finished_row_count,
            )
        };
        let row_count = usize::from(row_count);
        let mut offset = header_bytes;
        if row_count > usize::from(MAX_EXECUTOR_BATCH_CANDIDATES) {
            return Err(ExecutorRequestError::InvalidPayload);
        }
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let row_header = payload
                .get(offset..)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            let (handle_bytes, encoded_cell_count) =
                generated::decode_executor_batch_row_record(row_header)
                    .ok_or(ExecutorRequestError::InvalidPayload)?;
            offset = offset
                .checked_add(generated::EXECUTOR_BATCH_ROW_RECORD_HEADER_BYTES as usize)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            let handle = ExecutorRowHandle::from_bytes(&handle_bytes)
                .ok_or(ExecutorRequestError::InvalidPayload)?;
            let cell_count = usize::from(encoded_cell_count);
            if cell_count > crate::tuple::MAX_COLUMNS {
                return Err(ExecutorRequestError::InvalidPayload);
            }
            let mut cells = Vec::with_capacity(cell_count);
            for _ in 0..cell_count {
                let state_bytes = payload
                    .get(offset..)
                    .and_then(|bytes| {
                        bytes.get(..generated::SANITIZED_CELL_TAG_RECORD_HEADER_BYTES as usize)
                    })
                    .ok_or(ExecutorRequestError::InvalidPayload)?;
                let state = generated::decode_sanitized_cell_tag_record(state_bytes)
                    .ok_or(ExecutorRequestError::InvalidPayload)?;
                offset = offset
                    .checked_add(generated::SANITIZED_CELL_TAG_RECORD_HEADER_BYTES as usize)
                    .ok_or(ExecutorRequestError::InvalidPayload)?;
                cells.push(match state {
                    0 => SanitizedCell::Value(decode_scalar_value_record(payload, &mut offset)?),
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
        SanitizedCell::Null => {
            payload.extend_from_slice(&generated::encode_sanitized_cell_tag_record(1))
        }
        SanitizedCell::Redacted => {
            payload.extend_from_slice(&generated::encode_sanitized_cell_tag_record(2))
        }
        SanitizedCell::Value(value) => {
            payload.extend_from_slice(&generated::encode_sanitized_cell_tag_record(0));
            let (type_code, bytes) = encode_scalar_value(value)?;
            append_scalar_value_record(payload, type_code, &bytes)?;
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

fn append_scalar_value_record(
    payload: &mut Vec<u8>,
    type_code: u8,
    bytes: &[u8],
) -> Result<(), ExecutorRequestError> {
    let value_length =
        u32::try_from(bytes.len()).map_err(|_| ExecutorRequestError::InvalidPayload)?;
    payload.extend_from_slice(&generated::encode_scalar_value_record(
        type_code,
        value_length,
    ));
    let encoded_bytes = generated::encode_scalar_value_record_value_bytes(bytes)
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    payload.extend_from_slice(encoded_bytes);
    Ok(())
}

fn decode_scalar_value_record(
    payload: &[u8],
    offset: &mut usize,
) -> Result<ScalarValue, ExecutorRequestError> {
    let (type_code, encoded_length) = generated::decode_scalar_value_record(
        payload
            .get(*offset..)
            .ok_or(ExecutorRequestError::InvalidPayload)?,
    )
    .ok_or(ExecutorRequestError::InvalidPayload)?;
    *offset = offset
        .checked_add(generated::SCALAR_VALUE_RECORD_HEADER_BYTES as usize)
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    let value_length =
        usize::try_from(encoded_length).map_err(|_| ExecutorRequestError::InvalidPayload)?;
    let end = offset
        .checked_add(value_length)
        .filter(|end| *end <= payload.len())
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    let bytes =
        generated::decode_scalar_value_record_value_bytes(&payload[*offset..end], encoded_length)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
    let value = decode_scalar_value(type_code, bytes)?;
    *offset = end;
    Ok(value)
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

pub(crate) fn encode_visible_column(
    payload: &mut Vec<u8>,
    column: &VisibleColumn,
) -> Result<(), ExecutorRequestError> {
    if column.name.is_empty() {
        return Err(ExecutorRequestError::InvalidPayload);
    }
    let (type_code, parameters) = encode_scalar_type(column.scalar_type);
    payload.extend_from_slice(&generated::encode_visible_column_record(
        column.column_id,
        column.ordinal,
        u8::from(column.nullable),
        type_code,
        parameters,
    ));
    let name = generated::encode_visible_column_record_column_name(&column.name)
        .ok_or(ExecutorRequestError::ResponseTooLarge)?;
    payload.extend_from_slice(&name);
    ensure_response_size(payload)
}

pub(crate) fn decode_visible_column(
    payload: &[u8],
    offset: &mut usize,
) -> Result<VisibleColumn, ExecutorRequestError> {
    let record = payload
        .get(*offset..)
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    let (column_id, ordinal, nullable, type_code, parameters) =
        generated::decode_visible_column_record(record)
            .ok_or(ExecutorRequestError::InvalidPayload)?;
    *offset = offset
        .checked_add(generated::VISIBLE_COLUMN_RECORD_HEADER_BYTES as usize)
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    let (name, name_bytes) = generated::decode_visible_column_record_column_name(
        payload
            .get(*offset..)
            .ok_or(ExecutorRequestError::InvalidPayload)?,
    )
    .ok_or(ExecutorRequestError::InvalidPayload)?;
    if name.is_empty() {
        return Err(ExecutorRequestError::InvalidPayload);
    }
    let name = name.to_owned();
    *offset = offset
        .checked_add(name_bytes)
        .ok_or(ExecutorRequestError::InvalidPayload)?;
    if nullable > 1 {
        return Err(ExecutorRequestError::InvalidPayload);
    }
    let scalar_type = decode_scalar_type(type_code, parameters)?;
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
    let result = dispatch_executor_request_inner(role, connection, request);
    if matches!(
        &result,
        Err(ExecutorRequestError::Transaction(
            TransactionError::Authorization(crate::authorization::AuthorizationError::Denied)
        ))
    ) {
        connection.record_policy_denial();
    }
    result
}

fn dispatch_executor_request_inner(
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
            crate::planner_ipc::DescribePlanRelationResponse {
                relation,
                proven_empty: false,
                visible_rows_estimate: None,
            }
            .encode_executor_identity()
            .map_err(|_| ExecutorRequestError::InvalidPayload)?
        }
        Operation::OpenScan => {
            let decoded = OpenScanRequest::decode(&request.payload)?;
            OpenScanResponse {
                cursor: connection.open_relation_scan(&decoded.relation, &decoded.column_ids)?,
            }
            .encode()
        }
        Operation::ProbeUniqueIndex => {
            let decoded = ProbeUniqueIndexRequest::decode(&request.payload)?;
            let values = decoded.values.iter().cloned().map(Some).collect::<Vec<_>>();
            let response = match connection.probe_relation_unique_index(
                &decoded.relation,
                decoded.index_id,
                &values,
                &decoded.column_ids,
            )? {
                ExecutorUniqueIndexProbe::Unavailable => ProbeUniqueIndexResponse::Unavailable,
                ExecutorUniqueIndexProbe::NoMatch => ProbeUniqueIndexResponse::NoMatch,
                ExecutorUniqueIndexProbe::Match { handle, cells } => {
                    ProbeUniqueIndexResponse::Match(ExecutorBatchRow { handle, cells })
                }
            };
            response.encode()?
        }
        Operation::ScanIndexRange => {
            let decoded = ScanIndexRangeRequest::decode(&request.payload)?;
            let response = match connection.scan_relation_index_range(
                &decoded.relation,
                decoded.index_id,
                decoded.lower,
                decoded.upper,
                &decoded.column_ids,
            )? {
                ExecutorIndexRangeScan::Unavailable => ScanIndexRangeResponse::Unavailable,
                ExecutorIndexRangeScan::Rows(rows) => ScanIndexRangeResponse::Rows(rows),
            };
            response.encode()?
        }
        Operation::ScanIndexPrefix => {
            let decoded = ScanIndexPrefixRequest::decode(&request.payload)?;
            let response = match connection.scan_relation_index_prefix(
                &decoded.relation,
                decoded.index_id,
                &decoded.values,
                &decoded.column_ids,
            )? {
                ExecutorIndexRangeScan::Unavailable => ScanIndexRangeResponse::Unavailable,
                ExecutorIndexRangeScan::Rows(rows) => ScanIndexRangeResponse::Rows(rows),
            };
            response.encode()?
        }
        Operation::ScanIndexRanges => {
            let decoded = ScanIndexRangesRequest::decode(&request.payload)?;
            let probes = decoded
                .probes
                .iter()
                .map(|probe| probe.values.clone())
                .collect::<Vec<_>>();
            let response = match connection.scan_relation_index_ranges(
                &decoded.relation,
                decoded.index_id,
                &probes,
                &decoded.column_ids,
            )? {
                Some(groups) => ScanIndexRangesResponse::Groups(groups),
                None => ScanIndexRangesResponse::Unavailable,
            };
            response.encode()?
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
            MutationAcknowledgement.encode()
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
    generated::encode_executor_cancellation_check_response(u8::from(cancelled)).to_vec()
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
    use super::{
        ExecutorBatchRow, ProbeUniqueIndexRequest, ProbeUniqueIndexResponse,
        ScanIndexPrefixRequest, ScanIndexRangeRequest, ScanIndexRangeResponse,
        encode_cancellation_check_response,
    };
    use crate::authorization::SanitizedCell;
    use crate::metadata_discovery::{ExecutorRowHandle, ResolvedRelationHandle};
    use crate::transaction::AuthorizedIndexRangeBound;
    use crate::tuple::ScalarValue;

    #[test]
    fn cancellation_check_response_matches_wire_bytes() {
        assert_eq!(encode_cancellation_check_response(false), [0, 1, 0]);
        assert_eq!(encode_cancellation_check_response(true), [0, 1, 1]);
    }

    #[test]
    fn unique_probe_request_and_response_round_trip_without_raw_row_ids() {
        let mut relation_bytes = [0u8; 16];
        relation_bytes[0] = 6;
        relation_bytes[1] = 1;
        let request = ProbeUniqueIndexRequest {
            relation: ResolvedRelationHandle::from_bytes(&relation_bytes).unwrap(),
            index_id: [9; 16],
            values: vec![ScalarValue::Integer(-17)],
            column_ids: vec![[7; 16]],
        };
        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[..2], &[0, 1]);
        assert_eq!(&encoded[2..18], &relation_bytes);
        assert_eq!(&encoded[18..34], &[9; 16]);
        assert_eq!(&encoded[34..38], &[0, 1, 0, 1]);
        assert_eq!(ProbeUniqueIndexRequest::decode(&encoded).unwrap(), request);

        for response in [
            ProbeUniqueIndexResponse::Unavailable,
            ProbeUniqueIndexResponse::NoMatch,
            ProbeUniqueIndexResponse::Match(ExecutorBatchRow {
                handle: {
                    let mut bytes = [0; 16];
                    bytes[0] = 8;
                    bytes[1] = 3;
                    ExecutorRowHandle::from_bytes(&bytes).unwrap()
                },
                cells: vec![SanitizedCell::Value(ScalarValue::Integer(-17))],
            }),
        ] {
            let bytes = response.encode().unwrap();
            assert_eq!(
                &bytes[..2],
                match &response {
                    ProbeUniqueIndexResponse::Unavailable => &[1, 0],
                    ProbeUniqueIndexResponse::NoMatch => &[1, 1],
                    ProbeUniqueIndexResponse::Match(_) => &[1, 2],
                }
            );
            assert_eq!(ProbeUniqueIndexResponse::decode(&bytes).unwrap(), response);
        }
        assert!(ProbeUniqueIndexRequest::decode(&encoded[..37]).is_err());
        assert!(ProbeUniqueIndexResponse::decode(&[1, 2]).is_err());
    }

    #[test]
    fn bounded_range_codec_preserves_bounds_and_sanitized_rows() {
        let mut relation_bytes = [0u8; 16];
        relation_bytes[0] = 6;
        relation_bytes[1] = 1;
        let request = ScanIndexRangeRequest {
            relation: ResolvedRelationHandle::from_bytes(&relation_bytes).unwrap(),
            index_id: [9; 16],
            lower: Some(AuthorizedIndexRangeBound {
                value: ScalarValue::Integer(5),
                inclusive: true,
            }),
            upper: Some(AuthorizedIndexRangeBound {
                value: ScalarValue::Integer(20),
                inclusive: false,
            }),
            column_ids: vec![[7; 16]],
        };
        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[..2], &[0, 1]);
        assert_eq!(&encoded[2..18], &relation_bytes);
        assert_eq!(&encoded[18..34], &[9; 16]);
        assert_eq!(encoded[34], 1);
        assert_eq!(encoded[44], 2);
        assert_eq!(&encoded[54..56], &[0, 1]);
        assert_eq!(ScanIndexRangeRequest::decode(&encoded).unwrap(), request);
        assert!(ScanIndexRangeRequest::decode(&encoded[..37]).is_err());
        let mut invalid_bound = encoded.clone();
        invalid_bound[34] = 3;
        assert!(ScanIndexRangeRequest::decode(&invalid_bound).is_err());
        let mut trailing = encoded.clone();
        trailing.push(0);
        assert!(ScanIndexRangeRequest::decode(&trailing).is_err());

        let response = ScanIndexRangeResponse::Rows(vec![vec![
            SanitizedCell::Value(ScalarValue::Integer(12)),
            SanitizedCell::Redacted,
        ]]);
        let encoded = response.encode().unwrap();
        assert_eq!(&encoded[..4], &[1, 1, 0, 1]);
        assert_eq!(&encoded[4..6], &[0, 2]);
        assert_eq!(ScanIndexRangeResponse::decode(&encoded).unwrap(), response);
        let unavailable = ScanIndexRangeResponse::Unavailable.encode().unwrap();
        assert_eq!(unavailable, [1, 0]);
        assert_eq!(
            ScanIndexRangeResponse::decode(&unavailable).unwrap(),
            ScanIndexRangeResponse::Unavailable
        );
        assert!(ScanIndexRangeResponse::decode(&[1, 2]).is_err());
        let oversized = ScanIndexRangeResponse::Rows(vec![vec![SanitizedCell::Value(
            ScalarValue::Text("x".repeat(crate::ipc::MAX_PAYLOAD_BYTES)),
        )]]);
        assert_eq!(
            ScanIndexRangeResponse::decode(&oversized.encode().unwrap()).unwrap(),
            ScanIndexRangeResponse::Unavailable
        );
    }

    #[test]
    fn composite_index_prefix_request_round_trips_typed_values_and_projection() {
        let mut relation_bytes = [0u8; 16];
        relation_bytes[0] = 6;
        relation_bytes[1] = 1;
        let request = ScanIndexPrefixRequest {
            relation: ResolvedRelationHandle::from_bytes(&relation_bytes).unwrap(),
            index_id: [9; 16],
            values: vec![ScalarValue::Integer(-17), ScalarValue::Text("north".into())],
            column_ids: vec![[7; 16], [8; 16]],
        };
        let encoded = request.encode().unwrap();
        assert_eq!(ScanIndexPrefixRequest::decode(&encoded).unwrap(), request);
        assert!(ScanIndexPrefixRequest::decode(&encoded[..encoded.len() - 1]).is_err());
        let mut invalid = encoded;
        invalid[34] = 1;
        assert!(ScanIndexPrefixRequest::decode(&invalid).is_err());
    }

    #[test]
    fn index_range_bounds_roundtrip_every_sql_scalar_type() {
        let mut relation_bytes = [0u8; 16];
        relation_bytes[0] = 6;
        relation_bytes[1] = 1;
        let values = [
            ScalarValue::Boolean(true),
            ScalarValue::SmallInt(-31),
            ScalarValue::Integer(-700),
            ScalarValue::BigInt(-9_000_000_001),
            ScalarValue::Decimal(123_456),
            ScalarValue::Text("equality probe".to_owned()),
            ScalarValue::Varchar("varchar probe".to_owned()),
            ScalarValue::Bytea(vec![0, 1, 0xff]),
            ScalarValue::Uuid([0x42; 16]),
            ScalarValue::Date(20_000),
            ScalarValue::TimestampTz(1_700_000_000_000_000),
        ];
        for value in values {
            let request = ScanIndexRangeRequest {
                relation: ResolvedRelationHandle::from_bytes(&relation_bytes).unwrap(),
                index_id: [9; 16],
                lower: Some(AuthorizedIndexRangeBound {
                    value: value.clone(),
                    inclusive: true,
                }),
                upper: Some(AuthorizedIndexRangeBound {
                    value,
                    inclusive: true,
                }),
                column_ids: vec![[7; 16]],
            };
            let encoded = request.encode().unwrap();
            assert_eq!(ScanIndexRangeRequest::decode(&encoded).unwrap(), request);
        }
    }
}
