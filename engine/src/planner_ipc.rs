//! Scoped planner metadata and estimate operations over trusted storage APIs.

use crate::executor_ipc::{
    ExecutorRequestError, ResolveRelationRequest, decode_visible_column, encode_visible_column,
    ensure_response_size,
};
use crate::ipc::{DispatchError, IpcFrame, Operation, ServiceRole, authorize_operation};
use crate::metadata_discovery::{
    AuthorizedPlanRelation, AuthorizedRangeAccessPath, AuthorizedRelationDescription,
    AuthorizedUniqueAccessPath, PlanningTableHandle,
};
pub use crate::query_abi::{
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_RELATION_ID_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_SCHEMA_ID_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_SCHEMA_VERSION_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_STATUS_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_TABLE_EPOCH_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_VERSION_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_UNAVAILABLE_RESPONSE_HEADER_BYTES,
    PLANNER_DESCRIBE_PLAN_RELATION_UNAVAILABLE_RESPONSE_STATUS_OFFSET,
    PLANNER_DESCRIBE_PLAN_RELATION_UNAVAILABLE_RESPONSE_VERSION_OFFSET,
    PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES,
    PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_PLANNING_HANDLE_OFFSET,
    PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_RELATION_ID_OFFSET,
    PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_STATUS_OFFSET,
    PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_VERSION_OFFSET,
    PLANNER_DESCRIBE_RELATION_UNAVAILABLE_RESPONSE_HEADER_BYTES,
    PLANNER_DESCRIBE_RELATION_UNAVAILABLE_RESPONSE_STATUS_OFFSET,
    PLANNER_DESCRIBE_RELATION_UNAVAILABLE_RESPONSE_VERSION_OFFSET,
    PLANNER_ESTIMATE_SCAN_REQUEST_COLUMN_COUNT_OFFSET, PLANNER_ESTIMATE_SCAN_REQUEST_HEADER_BYTES,
    PLANNER_ESTIMATE_SCAN_REQUEST_PLANNING_HANDLE_OFFSET,
    PLANNER_ESTIMATE_SCAN_REQUEST_VERSION_OFFSET,
    PLANNER_ESTIMATE_SCAN_RESPONSE_COLUMN_COUNT_OFFSET,
    PLANNER_ESTIMATE_SCAN_RESPONSE_HEADER_BYTES,
    PLANNER_ESTIMATE_SCAN_RESPONSE_VISIBLE_ROWS_OFFSET,
};
use crate::statistics::{AuthorizedScanEstimate, AuthorizedScanSummary, ColumnPopulation};
use crate::transaction::{PlannerMetadataConnection, TransactionError};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

const DESCRIBE_HEADER_BYTES: usize =
    PLANNER_DESCRIBE_RELATION_UNAVAILABLE_RESPONSE_HEADER_BYTES as usize;
const ESTIMATE_HEADER_BYTES: usize = PLANNER_ESTIMATE_SCAN_REQUEST_HEADER_BYTES as usize;
pub const MAX_PLANNER_REQUESTS_PER_CONNECTION: usize = 256;

#[derive(Debug)]
pub enum PlannerRequestError {
    InvalidPayload,
    ResponseTooLarge,
    Dispatch(DispatchError),
    Transaction(TransactionError),
}

#[derive(Debug)]
pub enum PlannerConnectionError {
    Frame(crate::ipc::FrameError),
    Request(PlannerRequestError),
    Abort(TransactionError),
    RequestLimitExceeded,
    DuplicateRequestId,
    SharedInvocationUnavailable,
    SharedInvocationPoisoned,
}

impl std::fmt::Display for PlannerRequestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "planner request failed: {self:?}")
    }
}

impl std::error::Error for PlannerRequestError {}

impl From<ExecutorRequestError> for PlannerRequestError {
    fn from(error: ExecutorRequestError) -> Self {
        match error {
            ExecutorRequestError::InvalidPayload => Self::InvalidPayload,
            ExecutorRequestError::ResponseTooLarge => Self::ResponseTooLarge,
            ExecutorRequestError::Dispatch(error) => Self::Dispatch(error),
            ExecutorRequestError::Transaction(error) => Self::Transaction(error),
            _ => Self::InvalidPayload,
        }
    }
}

impl From<TransactionError> for PlannerRequestError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribeRelationResponse {
    pub table: Option<(PlanningTableHandle, AuthorizedRelationDescription)>,
}

/// Authorized stable relation identity for optimizer binding. This response
/// contains no connection-local planner token and grants no executor access.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribePlanRelationResponse {
    pub relation: Option<AuthorizedPlanRelation>,
    /// Storage-proven empty authorized population, omitted from executor
    /// identity responses and valid only for optimizer planning.
    pub proven_empty: bool,
    /// Exact authorized-view row count when a bounded estimate was requested.
    pub visible_rows_estimate: Option<u64>,
}

const MAX_PLAN_UNIQUE_ACCESS_PATHS: usize = 256;
const RANGE_ACCESS_PATHS_MARKER: [u8; 2] = [0x52, 0x01];
const EMPTY_POPULATION_MARKER: [u8; 2] = [0x52, 0x02];
const SCAN_ESTIMATE_REQUEST_MARKER: [u8; 2] = [0x52, 0x03];
const POPULATION_ESTIMATE_LIMIT_MARKER: [u8; 2] = [0x52, 0x07];
const JOIN_ESTIMATE_REQUEST_MARKER: [u8; 2] = [0x52, 0x08];
const JOIN_ESTIMATE_RESPONSE_MARKER: [u8; 2] = [0x52, 0x09];
const CARDINALITY_ESTIMATE_MARKER: [u8; 2] = [0x52, 0x04];
const PREDICATE_ESTIMATE_REQUEST_MARKER: [u8; 2] = [0x52, 0x05];
const PREDICATE_ESTIMATE_RESPONSE_MARKER: [u8; 2] = [0x52, 0x06];

/// Relation lookup request with an explicit opt-in for a bounded authorized
/// scan estimate. Ordinary metadata requests never spend estimate allowance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DescribePlanRelationRequest {
    pub schema_name: String,
    pub relation_name: String,
    pub include_scan_estimate: bool,
    /// A larger bounded row-count probe requested by join planning.
    pub population_estimate_max_rows: Option<u64>,
}

impl DescribePlanRelationRequest {
    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        let mut payload = ResolveRelationRequest {
            schema_name: self.schema_name.clone(),
            relation_name: self.relation_name.clone(),
        }
        .encode()?;
        if self.include_scan_estimate && self.population_estimate_max_rows.is_some() {
            return Err(PlannerRequestError::InvalidPayload);
        }
        if self.include_scan_estimate {
            payload.extend_from_slice(&SCAN_ESTIMATE_REQUEST_MARKER);
            payload.push(1);
        } else if let Some(maximum_rows) = self.population_estimate_max_rows {
            if maximum_rows <= crate::optimizer_plan::SMALL_POPULATION_SCAN_ROWS
                || maximum_rows > crate::optimizer_plan::JOIN_POPULATION_ESTIMATE_MAX_ROWS
            {
                return Err(PlannerRequestError::InvalidPayload);
            }
            payload.extend_from_slice(&POPULATION_ESTIMATE_LIMIT_MARKER);
            payload.extend_from_slice(
                &u16::try_from(maximum_rows)
                    .map_err(|_| PlannerRequestError::InvalidPayload)?
                    .to_be_bytes(),
            );
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        let (base, include_scan_estimate, population_estimate_max_rows) = if payload.len() >= 4
            && payload[payload.len() - 4..payload.len() - 2] == POPULATION_ESTIMATE_LIMIT_MARKER
        {
            let maximum_rows = u64::from(u16::from_be_bytes(
                payload[payload.len() - 2..]
                    .try_into()
                    .map_err(|_| PlannerRequestError::InvalidPayload)?,
            ));
            if maximum_rows <= crate::optimizer_plan::SMALL_POPULATION_SCAN_ROWS
                || maximum_rows > crate::optimizer_plan::JOIN_POPULATION_ESTIMATE_MAX_ROWS
            {
                return Err(PlannerRequestError::InvalidPayload);
            }
            (&payload[..payload.len() - 4], false, Some(maximum_rows))
        } else if payload.len() >= 3
            && payload[payload.len() - 3..payload.len() - 1] == SCAN_ESTIMATE_REQUEST_MARKER
        {
            if payload[payload.len() - 1] != 1 {
                return Err(PlannerRequestError::InvalidPayload);
            }
            (&payload[..payload.len() - 3], true, None)
        } else {
            (payload, false, None)
        };
        let decoded = ResolveRelationRequest::decode(base)?;
        Ok(Self {
            schema_name: decoded.schema_name,
            relation_name: decoded.relation_name,
            include_scan_estimate,
            population_estimate_max_rows,
        })
    }
}

impl DescribePlanRelationResponse {
    /// Preserve the executor's established identity-only response for C SDK
    /// consumers. Approved paths are returned on the planner channel only.
    pub fn encode_executor_identity(&self) -> Result<Vec<u8>, PlannerRequestError> {
        let relation = self.relation.clone().map(|mut relation| {
            relation.unique_access_paths.clear();
            relation.range_access_paths.clear();
            relation
        });
        Self {
            relation,
            proven_empty: false,
            visible_rows_estimate: None,
        }
        .encode()
    }

    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        match &self.relation {
            None => {
                if self.proven_empty || self.visible_rows_estimate.is_some() {
                    return Err(PlannerRequestError::InvalidPayload);
                }
                let payload =
                    crate::query_abi::encode_planner_describe_plan_relation_unavailable_response(1)
                        .to_vec();
                ensure_response_size(&payload)?;
                Ok(payload)
            }
            Some(relation) => {
                if relation.schema_id == [0; 16]
                    || relation.schema_version == 0
                    || relation.table_epoch == 0
                    || relation.relation.relation_id == [0; 16]
                    || relation.relation.name.is_empty()
                    || relation.relation.columns.is_empty()
                    || relation.relation.columns.len() > crate::tuple::MAX_COLUMNS
                    || relation.unique_access_paths.len() > MAX_PLAN_UNIQUE_ACCESS_PATHS
                    || relation.range_access_paths.len() > MAX_PLAN_UNIQUE_ACCESS_PATHS
                {
                    return Err(PlannerRequestError::InvalidPayload);
                }
                let mut payload =
                    crate::query_abi::encode_planner_describe_plan_relation_available_response(
                        0,
                        relation.schema_id,
                        relation.schema_version,
                        relation.table_epoch,
                        relation.relation.relation_id,
                    )
                    .to_vec();
                let count = u16::try_from(relation.relation.columns.len())
                    .map_err(|_| PlannerRequestError::ResponseTooLarge)?;
                let relation_name = crate::query_abi::encode_planner_describe_plan_relation_available_response_relation_name(
                    &relation.relation.name,
                )
                .ok_or(PlannerRequestError::ResponseTooLarge)?;
                payload.extend_from_slice(&relation_name);
                payload.extend_from_slice(
                    &crate::query_abi::encode_planner_describe_plan_relation_available_response_column_count(
                        count,
                    ),
                );
                for column in &relation.relation.columns {
                    encode_visible_column(&mut payload, column)?;
                }
                if !relation.unique_access_paths.is_empty() {
                    payload.extend_from_slice(
                        &u16::try_from(relation.unique_access_paths.len())
                            .map_err(|_| PlannerRequestError::ResponseTooLarge)?
                            .to_be_bytes(),
                    );
                }
                let mut seen_indexes = std::collections::HashSet::new();
                for path in &relation.unique_access_paths {
                    if path.index_id == [0; 16]
                        || !seen_indexes.insert(path.index_id)
                        || path.column_ids.is_empty()
                        || path.column_ids.len() > crate::tuple::MAX_COLUMNS
                        || path.column_ids.contains(&[0; 16])
                        || path
                            .column_ids
                            .iter()
                            .copied()
                            .collect::<std::collections::HashSet<_>>()
                            .len()
                            != path.column_ids.len()
                        || path.column_ids.iter().any(|column_id| {
                            !relation
                                .relation
                                .columns
                                .iter()
                                .any(|column| column.column_id == *column_id)
                        })
                    {
                        return Err(PlannerRequestError::InvalidPayload);
                    }
                    payload.extend_from_slice(&path.index_id);
                    payload.extend_from_slice(
                        &u16::try_from(path.column_ids.len())
                            .map_err(|_| PlannerRequestError::ResponseTooLarge)?
                            .to_be_bytes(),
                    );
                    for column_id in &path.column_ids {
                        payload.extend_from_slice(column_id);
                    }
                }
                if !relation.range_access_paths.is_empty() {
                    payload.extend_from_slice(&RANGE_ACCESS_PATHS_MARKER);
                    payload.extend_from_slice(
                        &u16::try_from(relation.range_access_paths.len())
                            .map_err(|_| PlannerRequestError::ResponseTooLarge)?
                            .to_be_bytes(),
                    );
                }
                let mut seen_range_indexes = std::collections::HashSet::new();
                for path in &relation.range_access_paths {
                    if path.index_id == [0; 16]
                        || !seen_range_indexes.insert(path.index_id)
                        || path.column_ids.is_empty()
                        || path.column_ids.len() > crate::tuple::MAX_COLUMNS
                        || path.column_ids.contains(&[0; 16])
                        || path
                            .column_ids
                            .iter()
                            .copied()
                            .collect::<std::collections::HashSet<_>>()
                            .len()
                            != path.column_ids.len()
                        || path.column_ids.iter().any(|column_id| {
                            !relation
                                .relation
                                .columns
                                .iter()
                                .any(|column| column.column_id == *column_id)
                        })
                        || relation
                            .unique_access_paths
                            .iter()
                            .any(|unique| unique.index_id == path.index_id)
                    {
                        return Err(PlannerRequestError::InvalidPayload);
                    }
                    payload.extend_from_slice(&path.index_id);
                    payload.extend_from_slice(
                        &u16::try_from(path.column_ids.len())
                            .map_err(|_| PlannerRequestError::ResponseTooLarge)?
                            .to_be_bytes(),
                    );
                    for column_id in &path.column_ids {
                        payload.extend_from_slice(column_id);
                    }
                }
                if let Some(visible_rows) = self.visible_rows_estimate {
                    payload.extend_from_slice(&CARDINALITY_ESTIMATE_MARKER);
                    payload.extend_from_slice(&visible_rows.to_be_bytes());
                }
                if self.proven_empty {
                    payload.extend_from_slice(&EMPTY_POPULATION_MARKER);
                    payload.push(1);
                }
                ensure_response_size(&payload)?;
                Ok(payload)
            }
        }
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if payload.len()
            == PLANNER_DESCRIBE_PLAN_RELATION_UNAVAILABLE_RESPONSE_HEADER_BYTES as usize
        {
            return match crate::query_abi::decode_planner_describe_plan_relation_unavailable_response(payload) {
                Some(1) => Ok(Self {
                    relation: None,
                    proven_empty: false,
                    visible_rows_estimate: None,
                }),
                _ => Err(PlannerRequestError::InvalidPayload),
            };
        }
        let (status, schema_id, schema_version, table_epoch, relation_id) =
            crate::query_abi::decode_planner_describe_plan_relation_available_response(payload)
                .ok_or(PlannerRequestError::InvalidPayload)?;
        if status != 0
            || payload.len()
                < PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES as usize + 5
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        {
            if schema_id == [0; 16]
                || schema_version == 0
                || table_epoch == 0
                || relation_id == [0; 16]
            {
                return Err(PlannerRequestError::InvalidPayload);
            }
            let mut offset =
                PLANNER_DESCRIBE_PLAN_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES as usize;
            let (name, name_bytes) = crate::query_abi::decode_planner_describe_plan_relation_available_response_relation_name(
                payload
                    .get(offset..)
                    .ok_or(PlannerRequestError::InvalidPayload)?,
            )
            .ok_or(PlannerRequestError::InvalidPayload)?;
            let name = name.to_owned();
            if name.is_empty() {
                return Err(PlannerRequestError::InvalidPayload);
            }
            offset = offset
                .checked_add(name_bytes)
                .ok_or(PlannerRequestError::InvalidPayload)?;
            let count = usize::from(
                crate::query_abi::decode_planner_describe_plan_relation_available_response_column_count(
                    payload
                        .get(offset..)
                        .ok_or(PlannerRequestError::InvalidPayload)?,
                )
                .ok_or(PlannerRequestError::InvalidPayload)?,
            );
            offset += 2;
            if count == 0 || count > crate::tuple::MAX_COLUMNS {
                return Err(PlannerRequestError::InvalidPayload);
            }
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                columns.push(decode_visible_column(payload, &mut offset)?);
            }
            let has_range_marker =
                payload.get(offset..offset + 2) == Some(RANGE_ACCESS_PATHS_MARKER.as_slice());
            let has_empty_marker =
                payload.get(offset..offset + 2) == Some(EMPTY_POPULATION_MARKER.as_slice());
            let has_cardinality_marker =
                payload.get(offset..offset + 2) == Some(CARDINALITY_ESTIMATE_MARKER.as_slice());
            let path_count = if offset == payload.len()
                || has_range_marker
                || has_empty_marker
                || has_cardinality_marker
            {
                0
            } else {
                let count = usize::from(u16::from_be_bytes(
                    payload
                        .get(offset..offset + 2)
                        .ok_or(PlannerRequestError::InvalidPayload)?
                        .try_into()
                        .map_err(|_| PlannerRequestError::InvalidPayload)?,
                ));
                offset += 2;
                count
            };
            if path_count > MAX_PLAN_UNIQUE_ACCESS_PATHS {
                return Err(PlannerRequestError::InvalidPayload);
            }
            let mut unique_access_paths = Vec::with_capacity(path_count);
            let mut seen_indexes = std::collections::HashSet::with_capacity(path_count);
            for _ in 0..path_count {
                let index_id: [u8; 16] = payload
                    .get(offset..offset + 16)
                    .ok_or(PlannerRequestError::InvalidPayload)?
                    .try_into()
                    .map_err(|_| PlannerRequestError::InvalidPayload)?;
                offset += 16;
                let key_count = usize::from(u16::from_be_bytes(
                    payload
                        .get(offset..offset + 2)
                        .ok_or(PlannerRequestError::InvalidPayload)?
                        .try_into()
                        .map_err(|_| PlannerRequestError::InvalidPayload)?,
                ));
                offset += 2;
                if index_id == [0; 16]
                    || !seen_indexes.insert(index_id)
                    || key_count == 0
                    || key_count > crate::tuple::MAX_COLUMNS
                {
                    return Err(PlannerRequestError::InvalidPayload);
                }
                let mut column_ids = Vec::with_capacity(key_count);
                for _ in 0..key_count {
                    let column_id: [u8; 16] = payload
                        .get(offset..offset + 16)
                        .ok_or(PlannerRequestError::InvalidPayload)?
                        .try_into()
                        .map_err(|_| PlannerRequestError::InvalidPayload)?;
                    offset += 16;
                    if column_id == [0; 16]
                        || !columns.iter().any(|column| column.column_id == column_id)
                        || column_ids.contains(&column_id)
                    {
                        return Err(PlannerRequestError::InvalidPayload);
                    }
                    column_ids.push(column_id);
                }
                unique_access_paths.push(AuthorizedUniqueAccessPath {
                    index_id,
                    column_ids,
                });
            }
            let mut range_access_paths = Vec::new();
            if payload.get(offset..offset + 2) == Some(RANGE_ACCESS_PATHS_MARKER.as_slice()) {
                offset += RANGE_ACCESS_PATHS_MARKER.len();
                let range_count = usize::from(u16::from_be_bytes(
                    payload
                        .get(offset..offset + 2)
                        .ok_or(PlannerRequestError::InvalidPayload)?
                        .try_into()
                        .map_err(|_| PlannerRequestError::InvalidPayload)?,
                ));
                offset += 2;
                if range_count == 0 || range_count > MAX_PLAN_UNIQUE_ACCESS_PATHS {
                    return Err(PlannerRequestError::InvalidPayload);
                }
                let mut seen_range_indexes = std::collections::HashSet::with_capacity(range_count);
                for _ in 0..range_count {
                    let index_id: [u8; 16] = payload
                        .get(offset..offset + 16)
                        .ok_or(PlannerRequestError::InvalidPayload)?
                        .try_into()
                        .map_err(|_| PlannerRequestError::InvalidPayload)?;
                    offset += 16;
                    let key_count = usize::from(u16::from_be_bytes(
                        payload
                            .get(offset..offset + 2)
                            .ok_or(PlannerRequestError::InvalidPayload)?
                            .try_into()
                            .map_err(|_| PlannerRequestError::InvalidPayload)?,
                    ));
                    offset += 2;
                    if index_id == [0; 16]
                        || !seen_range_indexes.insert(index_id)
                        || unique_access_paths
                            .iter()
                            .any(|path| path.index_id == index_id)
                        || key_count == 0
                        || key_count > crate::tuple::MAX_COLUMNS
                    {
                        return Err(PlannerRequestError::InvalidPayload);
                    }
                    let mut column_ids = Vec::with_capacity(key_count);
                    for _ in 0..key_count {
                        let column_id: [u8; 16] = payload
                            .get(offset..offset + 16)
                            .ok_or(PlannerRequestError::InvalidPayload)?
                            .try_into()
                            .map_err(|_| PlannerRequestError::InvalidPayload)?;
                        offset += 16;
                        if column_id == [0; 16]
                            || !columns.iter().any(|column| column.column_id == column_id)
                            || column_ids.contains(&column_id)
                        {
                            return Err(PlannerRequestError::InvalidPayload);
                        }
                        column_ids.push(column_id);
                    }
                    range_access_paths.push(AuthorizedRangeAccessPath {
                        index_id,
                        column_ids,
                    });
                }
            }
            let visible_rows_estimate = if payload.get(offset..offset + 2)
                == Some(CARDINALITY_ESTIMATE_MARKER.as_slice())
            {
                offset += CARDINALITY_ESTIMATE_MARKER.len();
                let visible_rows = u64::from_be_bytes(
                    payload
                        .get(offset..offset + 8)
                        .ok_or(PlannerRequestError::InvalidPayload)?
                        .try_into()
                        .map_err(|_| PlannerRequestError::InvalidPayload)?,
                );
                offset += 8;
                Some(visible_rows)
            } else {
                None
            };
            let proven_empty =
                if payload.get(offset..offset + 2) == Some(EMPTY_POPULATION_MARKER.as_slice()) {
                    offset += EMPTY_POPULATION_MARKER.len();
                    let flag = *payload
                        .get(offset)
                        .ok_or(PlannerRequestError::InvalidPayload)?;
                    offset += 1;
                    if flag != 1 {
                        return Err(PlannerRequestError::InvalidPayload);
                    }
                    true
                } else {
                    false
                };
            if offset != payload.len() {
                return Err(PlannerRequestError::InvalidPayload);
            }
            Ok(Self {
                relation: Some(AuthorizedPlanRelation {
                    schema_id,
                    schema_version,
                    table_epoch,
                    relation: AuthorizedRelationDescription {
                        relation_id,
                        name,
                        columns,
                    },
                    unique_access_paths,
                    range_access_paths,
                }),
                proven_empty,
                visible_rows_estimate,
            })
        }
    }
}

impl DescribeRelationResponse {
    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        match &self.table {
            None => {
                let payload =
                    crate::query_abi::encode_planner_describe_relation_unavailable_response(1)
                        .to_vec();
                ensure_response_size(&payload)?;
                Ok(payload)
            }
            Some((handle, description)) => {
                if description.name.is_empty() {
                    return Err(PlannerRequestError::InvalidPayload);
                }
                let mut payload =
                    crate::query_abi::encode_planner_describe_relation_available_response(
                        0,
                        description.relation_id,
                        handle.to_bytes(),
                    )
                    .to_vec();
                let count = u16::try_from(description.columns.len())
                    .map_err(|_| PlannerRequestError::ResponseTooLarge)?;
                let relation_name = crate::query_abi::encode_planner_describe_relation_available_response_relation_name(
                    &description.name,
                )
                .ok_or(PlannerRequestError::ResponseTooLarge)?;
                payload.extend_from_slice(&relation_name);
                payload.extend_from_slice(
                    &crate::query_abi::encode_planner_describe_relation_available_response_column_count(
                        count,
                    ),
                );
                for column in &description.columns {
                    encode_visible_column(&mut payload, column)?;
                }
                ensure_response_size(&payload)?;
                Ok(payload)
            }
        }
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if payload.len() == DESCRIBE_HEADER_BYTES {
            return match crate::query_abi::decode_planner_describe_relation_unavailable_response(
                payload,
            ) {
                Some(1) => Ok(Self { table: None }),
                _ => Err(PlannerRequestError::InvalidPayload),
            };
        }
        let (status, relation_id, handle_bytes) =
            crate::query_abi::decode_planner_describe_relation_available_response(payload)
                .ok_or(PlannerRequestError::InvalidPayload)?;
        if status != 0
            || payload.len()
                < PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES as usize + 5
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        {
            let handle = PlanningTableHandle::from_bytes(&handle_bytes)
                .ok_or(PlannerRequestError::InvalidPayload)?;
            let mut offset = PLANNER_DESCRIBE_RELATION_AVAILABLE_RESPONSE_HEADER_BYTES as usize;
            let (name, name_bytes) = crate::query_abi::decode_planner_describe_relation_available_response_relation_name(
                payload
                    .get(offset..)
                    .ok_or(PlannerRequestError::InvalidPayload)?,
            )
            .ok_or(PlannerRequestError::InvalidPayload)?;
            let name = name.to_owned();
            if name.is_empty() {
                return Err(PlannerRequestError::InvalidPayload);
            }
            offset = offset
                .checked_add(name_bytes)
                .ok_or(PlannerRequestError::InvalidPayload)?;
            let count = usize::from(
                crate::query_abi::decode_planner_describe_relation_available_response_column_count(
                    payload
                        .get(offset..)
                        .ok_or(PlannerRequestError::InvalidPayload)?,
                )
                .ok_or(PlannerRequestError::InvalidPayload)?,
            );
            offset += 2;
            if count > crate::tuple::MAX_COLUMNS {
                return Err(PlannerRequestError::InvalidPayload);
            }
            let mut columns = Vec::with_capacity(count);
            for _ in 0..count {
                columns.push(decode_visible_column(payload, &mut offset)?);
            }
            if offset != payload.len() {
                return Err(PlannerRequestError::InvalidPayload);
            }
            Ok(Self {
                table: Some((
                    handle,
                    AuthorizedRelationDescription {
                        relation_id,
                        name,
                        columns,
                    },
                )),
            })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EstimateScanRequest {
    pub handle: PlanningTableHandle,
    /// Empty requests estimates for the full declared shape.
    pub column_ids: Vec<[u8; 16]>,
}

/// A bounded predicate estimate is bound to the same invocation by its
/// planner connection and re-resolves the named relation before evaluation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EstimatePredicateRequest {
    pub schema_name: String,
    pub relation_name: String,
    pub predicate_bytes: Vec<u8>,
}

/// Exact join-cardinality estimate request. Relation names and key IDs are
/// claims only; storage re-resolves both names in the invocation context.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EstimateJoinRequest {
    pub schema_name: String,
    pub left_relation_name: String,
    pub right_relation_name: String,
    pub left_key_column_ids: Vec<[u8; 16]>,
    pub right_key_column_ids: Vec<[u8; 16]>,
    pub maximum_rows: u64,
}

impl EstimateJoinRequest {
    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        let key_count = self.left_key_column_ids.len();
        if !(1..=8).contains(&key_count)
            || key_count != self.right_key_column_ids.len()
            || self.left_key_column_ids.contains(&[0; 16])
            || self.right_key_column_ids.contains(&[0; 16])
            || self.maximum_rows == 0
            || self.maximum_rows > crate::statistics::MAX_JOIN_ESTIMATE_ROWS
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let left = ResolveRelationRequest {
            schema_name: self.schema_name.clone(),
            relation_name: self.left_relation_name.clone(),
        }
        .encode()?;
        let right = ResolveRelationRequest {
            schema_name: self.schema_name.clone(),
            relation_name: self.right_relation_name.clone(),
        }
        .encode()?;
        let mut payload = Vec::with_capacity(13 + left.len() + right.len() + key_count * 32);
        payload.extend_from_slice(&JOIN_ESTIMATE_REQUEST_MARKER);
        payload.extend_from_slice(
            &u16::try_from(self.maximum_rows)
                .map_err(|_| PlannerRequestError::InvalidPayload)?
                .to_be_bytes(),
        );
        payload.push(key_count as u8);
        payload.extend_from_slice(
            &u32::try_from(left.len())
                .map_err(|_| PlannerRequestError::InvalidPayload)?
                .to_be_bytes(),
        );
        payload.extend_from_slice(
            &u32::try_from(right.len())
                .map_err(|_| PlannerRequestError::InvalidPayload)?
                .to_be_bytes(),
        );
        payload.extend_from_slice(&left);
        payload.extend_from_slice(&right);
        for (left_id, right_id) in self
            .left_key_column_ids
            .iter()
            .zip(&self.right_key_column_ids)
        {
            payload.extend_from_slice(left_id);
            payload.extend_from_slice(right_id);
        }
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if payload.len() < 13 || !payload.starts_with(&JOIN_ESTIMATE_REQUEST_MARKER) {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let maximum_rows = u64::from(u16::from_be_bytes(
            payload[2..4]
                .try_into()
                .map_err(|_| PlannerRequestError::InvalidPayload)?,
        ));
        let key_count = usize::from(payload[4]);
        if !(1..=8).contains(&key_count)
            || maximum_rows == 0
            || maximum_rows > crate::statistics::MAX_JOIN_ESTIMATE_ROWS
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let left_len = usize::try_from(u32::from_be_bytes(
            payload[5..9]
                .try_into()
                .map_err(|_| PlannerRequestError::InvalidPayload)?,
        ))
        .map_err(|_| PlannerRequestError::InvalidPayload)?;
        let right_len = usize::try_from(u32::from_be_bytes(
            payload[9..13]
                .try_into()
                .map_err(|_| PlannerRequestError::InvalidPayload)?,
        ))
        .map_err(|_| PlannerRequestError::InvalidPayload)?;
        let left_end = 13usize
            .checked_add(left_len)
            .ok_or(PlannerRequestError::InvalidPayload)?;
        let right_end = left_end
            .checked_add(right_len)
            .ok_or(PlannerRequestError::InvalidPayload)?;
        let key_bytes = key_count
            .checked_mul(32)
            .ok_or(PlannerRequestError::InvalidPayload)?;
        if right_end.checked_add(key_bytes) != Some(payload.len()) {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let left = ResolveRelationRequest::decode(&payload[13..left_end])?;
        let right = ResolveRelationRequest::decode(&payload[left_end..right_end])?;
        if left.schema_name != right.schema_name {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let mut left_key_column_ids = Vec::with_capacity(key_count);
        let mut right_key_column_ids = Vec::with_capacity(key_count);
        for pair in payload[right_end..].chunks_exact(32) {
            left_key_column_ids.push(
                pair[..16]
                    .try_into()
                    .map_err(|_| PlannerRequestError::InvalidPayload)?,
            );
            right_key_column_ids.push(
                pair[16..]
                    .try_into()
                    .map_err(|_| PlannerRequestError::InvalidPayload)?,
            );
        }
        if left_key_column_ids.contains(&[0; 16])
            || right_key_column_ids.contains(&[0; 16])
            || left_key_column_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != key_count
            || right_key_column_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != key_count
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        Ok(Self {
            schema_name: left.schema_name,
            left_relation_name: left.relation_name,
            right_relation_name: right.relation_name,
            left_key_column_ids,
            right_key_column_ids,
            maximum_rows,
        })
    }
}

impl EstimatePredicateRequest {
    pub fn from_bound_predicate(
        schema_name: String,
        relation_name: String,
        predicate: &crate::optimizer_binding::BoundPredicate,
    ) -> Result<Self, PlannerRequestError> {
        let predicate_bytes = crate::optimizer_plan::encode_bound_predicate_v1(predicate)
            .map_err(|_| PlannerRequestError::InvalidPayload)?;
        Ok(Self {
            schema_name,
            relation_name,
            predicate_bytes,
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        if self.predicate_bytes.is_empty()
            || self.predicate_bytes.len() > crate::optimizer_request::MAX_PREDICATE_BYTES
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let mut payload = ResolveRelationRequest {
            schema_name: self.schema_name.clone(),
            relation_name: self.relation_name.clone(),
        }
        .encode()?;
        payload.splice(0..0, PREDICATE_ESTIMATE_REQUEST_MARKER);
        payload.extend_from_slice(&self.predicate_bytes);
        payload.extend_from_slice(
            &u32::try_from(self.predicate_bytes.len())
                .map_err(|_| PlannerRequestError::InvalidPayload)?
                .to_be_bytes(),
        );
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if !payload.starts_with(&PREDICATE_ESTIMATE_REQUEST_MARKER) || payload.len() < 6 {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let body = &payload[PREDICATE_ESTIMATE_REQUEST_MARKER.len()..];
        let length_bytes: [u8; 4] = body[body.len() - 4..]
            .try_into()
            .map_err(|_| PlannerRequestError::InvalidPayload)?;
        let predicate_len = usize::try_from(u32::from_be_bytes(length_bytes))
            .map_err(|_| PlannerRequestError::InvalidPayload)?;
        if predicate_len == 0 || predicate_len > crate::optimizer_request::MAX_PREDICATE_BYTES {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let predicate_start = body
            .len()
            .checked_sub(4)
            .and_then(|end| end.checked_sub(predicate_len))
            .ok_or(PlannerRequestError::InvalidPayload)?;
        let base = ResolveRelationRequest::decode(&body[..predicate_start])?;
        Ok(Self {
            schema_name: base.schema_name,
            relation_name: base.relation_name,
            predicate_bytes: body[predicate_start..predicate_start + predicate_len].to_vec(),
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EstimatePredicateResponse {
    Exact {
        visible_rows: u64,
        matching_rows: u64,
    },
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EstimateJoinResponse {
    Exact { joined_rows: u64 },
    Unknown,
}

impl EstimateJoinResponse {
    pub fn encode(self) -> Result<Vec<u8>, PlannerRequestError> {
        match self {
            Self::Exact { joined_rows } => {
                let mut payload = Vec::with_capacity(10);
                payload.extend_from_slice(&JOIN_ESTIMATE_RESPONSE_MARKER);
                payload.extend_from_slice(&joined_rows.to_be_bytes());
                ensure_response_size(&payload)?;
                Ok(payload)
            }
            Self::Unknown => {
                let payload =
                    crate::query_abi::encode_planner_estimate_scan_unknown_response(1).to_vec();
                ensure_response_size(&payload)?;
                Ok(payload)
            }
        }
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if payload.starts_with(&JOIN_ESTIMATE_RESPONSE_MARKER) {
            if payload.len() != 10 {
                return Err(PlannerRequestError::InvalidPayload);
            }
            return Ok(Self::Exact {
                joined_rows: u64::from_be_bytes(
                    payload[2..10]
                        .try_into()
                        .map_err(|_| PlannerRequestError::InvalidPayload)?,
                ),
            });
        }
        match crate::query_abi::decode_planner_estimate_scan_unknown_response(payload) {
            Some(1) => Ok(Self::Unknown),
            _ => Err(PlannerRequestError::InvalidPayload),
        }
    }
}

impl EstimatePredicateResponse {
    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        let payload = match self {
            Self::Exact {
                visible_rows,
                matching_rows,
            } if matching_rows <= visible_rows => {
                let mut payload = Vec::with_capacity(18);
                payload.extend_from_slice(&PREDICATE_ESTIMATE_RESPONSE_MARKER);
                payload.extend_from_slice(&visible_rows.to_be_bytes());
                payload.extend_from_slice(&matching_rows.to_be_bytes());
                payload
            }
            Self::Exact { .. } => return Err(PlannerRequestError::InvalidPayload),
            Self::Unknown => {
                crate::query_abi::encode_planner_estimate_scan_unknown_response(1).to_vec()
            }
        };
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if payload.starts_with(&PREDICATE_ESTIMATE_RESPONSE_MARKER) {
            if payload.len() != 18 {
                return Err(PlannerRequestError::InvalidPayload);
            }
            let visible_rows = u64::from_be_bytes(
                payload[2..10]
                    .try_into()
                    .map_err(|_| PlannerRequestError::InvalidPayload)?,
            );
            let matching_rows = u64::from_be_bytes(
                payload[10..18]
                    .try_into()
                    .map_err(|_| PlannerRequestError::InvalidPayload)?,
            );
            if matching_rows > visible_rows {
                return Err(PlannerRequestError::InvalidPayload);
            }
            return Ok(Self::Exact {
                visible_rows,
                matching_rows,
            });
        }
        match crate::query_abi::decode_planner_estimate_scan_unknown_response(payload) {
            Some(1) => Ok(Self::Unknown),
            _ => Err(PlannerRequestError::InvalidPayload),
        }
    }
}

impl EstimateScanRequest {
    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
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
            return Err(PlannerRequestError::InvalidPayload);
        }
        let column_count = u16::try_from(self.column_ids.len())
            .map_err(|_| PlannerRequestError::InvalidPayload)?;
        let mut payload = crate::query_abi::encode_planner_estimate_scan_request(
            self.handle.to_bytes(),
            column_count,
        )
        .to_vec();
        let encoded_column_ids =
            crate::query_abi::encode_planner_estimate_scan_request_column_ids(&self.column_ids)
                .ok_or(PlannerRequestError::InvalidPayload)?;
        payload.extend_from_slice(&encoded_column_ids);
        ensure_response_size(&payload)?;
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        let (handle_bytes, encoded_column_count) =
            crate::query_abi::decode_planner_estimate_scan_request(payload)
                .ok_or(PlannerRequestError::InvalidPayload)?;
        let handle = PlanningTableHandle::from_bytes(&handle_bytes)
            .ok_or(PlannerRequestError::InvalidPayload)?;
        let count = usize::from(encoded_column_count);
        if count > crate::tuple::MAX_COLUMNS || payload.len() < ESTIMATE_HEADER_BYTES {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let column_ids = crate::query_abi::decode_planner_estimate_scan_request_column_ids(
            &payload[ESTIMATE_HEADER_BYTES..],
            encoded_column_count,
        )
        .ok_or(PlannerRequestError::InvalidPayload)?;
        if column_ids.contains(&[0; 16])
            || column_ids
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != column_ids.len()
        {
            return Err(PlannerRequestError::InvalidPayload);
        }
        Ok(Self { handle, column_ids })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EstimateScanResponse {
    Exact(AuthorizedScanSummary),
    Unknown,
}

impl EstimateScanResponse {
    pub fn from_estimate(estimate: AuthorizedScanEstimate) -> Self {
        match estimate {
            AuthorizedScanEstimate::Exact(summary) => Self::Exact(summary),
            AuthorizedScanEstimate::Unknown => Self::Unknown,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, PlannerRequestError> {
        match self {
            Self::Unknown => {
                let payload =
                    crate::query_abi::encode_planner_estimate_scan_unknown_response(1).to_vec();
                ensure_response_size(&payload)?;
                Ok(payload)
            }
            Self::Exact(summary) => {
                if summary.columns.len() > crate::tuple::MAX_COLUMNS {
                    return Err(PlannerRequestError::ResponseTooLarge);
                }
                let column_count = u16::try_from(summary.columns.len())
                    .map_err(|_| PlannerRequestError::ResponseTooLarge)?;
                let mut payload = crate::query_abi::encode_planner_estimate_scan_response(
                    0,
                    summary.visible_rows,
                    column_count,
                )
                .to_vec();
                let populations = summary
                    .columns
                    .iter()
                    .map(|column| [column.readable_values, column.nulls, column.redacted])
                    .collect::<Vec<_>>();
                let encoded_populations =
                    crate::query_abi::encode_planner_estimate_scan_response_column_populations(
                        &populations,
                    )
                    .ok_or(PlannerRequestError::ResponseTooLarge)?;
                payload.extend_from_slice(&encoded_populations);
                ensure_response_size(&payload)?;
                Ok(payload)
            }
        }
    }

    pub fn decode(payload: &[u8]) -> Result<Self, PlannerRequestError> {
        if crate::query_abi::decode_planner_estimate_scan_unknown_response(payload) == Some(1) {
            return Ok(Self::Unknown);
        }
        let (status, visible_rows, encoded_column_count) =
            crate::query_abi::decode_planner_estimate_scan_response(payload)
                .ok_or(PlannerRequestError::InvalidPayload)?;
        if status != 0 {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let count = usize::from(encoded_column_count);
        let header_bytes = PLANNER_ESTIMATE_SCAN_RESPONSE_HEADER_BYTES as usize;
        if count > crate::tuple::MAX_COLUMNS || payload.len() < header_bytes {
            return Err(PlannerRequestError::InvalidPayload);
        }
        let populations =
            crate::query_abi::decode_planner_estimate_scan_response_column_populations(
                &payload[header_bytes..],
                encoded_column_count,
            )
            .ok_or(PlannerRequestError::InvalidPayload)?;
        let columns = populations
            .into_iter()
            .map(|[readable_values, nulls, redacted]| ColumnPopulation {
                readable_values,
                nulls,
                redacted,
            })
            .collect();
        Ok(Self::Exact(AuthorizedScanSummary {
            visible_rows,
            columns,
        }))
    }
}

pub fn dispatch_planner_request(
    role: ServiceRole,
    connection: &mut PlannerMetadataConnection<'_, '_>,
    request: &IpcFrame,
) -> Result<IpcFrame, PlannerRequestError> {
    let operation =
        Operation::from_code(request.operation).map_err(PlannerRequestError::Dispatch)?;
    authorize_operation(role, operation).map_err(PlannerRequestError::Dispatch)?;
    let payload = match operation {
        Operation::DescribeRelation => {
            let decoded = ResolveRelationRequest::decode(&request.payload)?;
            DescribeRelationResponse {
                table: connection.resolve_relation(&decoded.schema_name, &decoded.relation_name)?,
            }
            .encode()?
        }
        Operation::EstimateScan => {
            if request
                .payload
                .starts_with(&PREDICATE_ESTIMATE_REQUEST_MARKER)
            {
                let decoded = EstimatePredicateRequest::decode(&request.payload)?;
                let estimate = connection.estimate_predicate_for_plan(
                    &decoded.schema_name,
                    &decoded.relation_name,
                    &decoded.predicate_bytes,
                )?;
                match estimate {
                    Some((visible_rows, matching_rows)) => EstimatePredicateResponse::Exact {
                        visible_rows,
                        matching_rows,
                    },
                    None => EstimatePredicateResponse::Unknown,
                }
                .encode()?
            } else if request.payload.starts_with(&JOIN_ESTIMATE_REQUEST_MARKER) {
                let decoded = EstimateJoinRequest::decode(&request.payload)?;
                let estimate = connection.estimate_join_cardinality_for_plan(
                    &decoded.schema_name,
                    &decoded.left_relation_name,
                    &decoded.right_relation_name,
                    &decoded.left_key_column_ids,
                    &decoded.right_key_column_ids,
                    decoded.maximum_rows,
                )?;
                match estimate {
                    Some(joined_rows) => EstimateJoinResponse::Exact { joined_rows },
                    None => EstimateJoinResponse::Unknown,
                }
                .encode()?
            } else {
                let decoded = EstimateScanRequest::decode(&request.payload)?;
                EstimateScanResponse::from_estimate(
                    connection.estimate_scan(&decoded.handle, &decoded.column_ids)?,
                )
                .encode()?
            }
        }
        Operation::DescribePlanRelation => {
            let decoded = DescribePlanRelationRequest::decode(&request.payload)?;
            let estimate_max_rows = decoded.population_estimate_max_rows.or_else(|| {
                decoded
                    .include_scan_estimate
                    .then_some(crate::optimizer_plan::SMALL_POPULATION_SCAN_ROWS)
            });
            let (relation, visible_rows_estimate) = if let Some(estimate_max_rows) =
                estimate_max_rows
            {
                if let Some((handle, relation)) = connection
                    .resolve_relation_for_plan(&decoded.schema_name, &decoded.relation_name)?
                {
                    let estimate =
                        connection.estimate_population_at_most(&handle, estimate_max_rows);
                    connection.close_relation(&handle)?;
                    (Some(relation), estimate?)
                } else {
                    (None, None)
                }
            } else {
                (
                    connection
                        .describe_relation_for_plan(&decoded.schema_name, &decoded.relation_name)?,
                    None,
                )
            };
            let proven_empty = visible_rows_estimate == Some(0);
            DescribePlanRelationResponse {
                relation,
                proven_empty,
                visible_rows_estimate,
            }
            .encode()?
        }
        _ => {
            return Err(PlannerRequestError::Dispatch(
                DispatchError::OperationNotAllowed { role, operation },
            ));
        }
    };
    Ok(IpcFrame {
        operation: request.operation,
        request_id: request.request_id,
        payload,
    })
}

/// Serve one planner stream bound out of band to a single invocation. Clean
/// EOF ends only this planning session so the invocation can continue with
/// execution; malformed frames and protocol violations abort it.
pub fn serve_planner<S: Read + Write>(
    stream: &mut S,
    transaction: &mut crate::transaction::InvocationTransaction<'_>,
) -> Result<(), PlannerConnectionError> {
    let mut connection = transaction.open_planner_metadata_connection();
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
                    Ok(()) => Err(PlannerConnectionError::Frame(error)),
                    Err(abort_error) => Err(PlannerConnectionError::Abort(abort_error)),
                };
            }
        };
        if request_ids.len() >= MAX_PLANNER_REQUESTS_PER_CONNECTION {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(PlannerConnectionError::RequestLimitExceeded),
                Err(error) => Err(PlannerConnectionError::Abort(error)),
            };
        }
        if !request_ids.insert(request.request_id) {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(PlannerConnectionError::DuplicateRequestId),
                Err(error) => Err(PlannerConnectionError::Abort(error)),
            };
        }
        let response =
            match dispatch_planner_request(ServiceRole::Planner, &mut connection, &request) {
                Ok(response) => response,
                Err(error) => {
                    drop(connection);
                    return match transaction.abort() {
                        Ok(()) => Err(PlannerConnectionError::Request(error)),
                        Err(abort_error) => Err(PlannerConnectionError::Abort(abort_error)),
                    };
                }
            };
        if let Err(error) = crate::ipc::write_frame(stream, &response) {
            drop(connection);
            return match transaction.abort() {
                Ok(()) => Err(PlannerConnectionError::Frame(error)),
                Err(abort_error) => Err(PlannerConnectionError::Abort(abort_error)),
            };
        }
    }
}

/// Serve a planner connection that shares an invocation with executor
/// sessions. The transaction lock is held only while one request is handled.
pub fn serve_planner_shared<S: Read + Write>(
    stream: &mut S,
    invocation: &Arc<Mutex<Option<crate::transaction::InvocationTransaction<'_>>>>,
) -> Result<(), PlannerConnectionError> {
    serve_planner_shared_with_observer(stream, invocation, |_, _| Ok(()))
}

/// Variant of the shared planner service that observes optimizer-planner
/// request headers before dispatch. This is used by the supervisor's bounded
/// channel instrumentation and deterministic service tests; callbacks run
/// without the invocation mutex held.
pub fn serve_planner_shared_with_observer<S, F>(
    stream: &mut S,
    invocation: &Arc<Mutex<Option<crate::transaction::InvocationTransaction<'_>>>>,
    mut on_request: F,
) -> Result<(), PlannerConnectionError>
where
    S: Read + Write,
    F: FnMut(u16, u64) -> Result<(), PlannerConnectionError>,
{
    let mut state = crate::transaction::PlannerMetadataSessionState::default();
    let mut request_ids = std::collections::HashSet::new();
    loop {
        let request = match crate::ipc::read_frame(stream) {
            Ok(Some(request)) => request,
            Ok(None) => return Ok(()),
            Err(error) => {
                abort_shared_planner_invocation(invocation)?;
                return Err(PlannerConnectionError::Frame(error));
            }
        };
        if request_ids.len() >= MAX_PLANNER_REQUESTS_PER_CONNECTION {
            abort_shared_planner_invocation(invocation)?;
            return Err(PlannerConnectionError::RequestLimitExceeded);
        }
        if !request_ids.insert(request.request_id) {
            abort_shared_planner_invocation(invocation)?;
            return Err(PlannerConnectionError::DuplicateRequestId);
        }
        if let Err(error) = on_request(request.operation, request.request_id) {
            abort_shared_planner_invocation(invocation)?;
            return Err(error);
        }
        let response = {
            let mut invocation = match invocation.lock() {
                Ok(invocation) => invocation,
                Err(poisoned) => {
                    let mut invocation = poisoned.into_inner();
                    if let Some(mut transaction) = invocation.take() {
                        let _ = transaction.abort();
                    }
                    return Err(PlannerConnectionError::SharedInvocationPoisoned);
                }
            };
            let transaction = invocation
                .as_mut()
                .ok_or(PlannerConnectionError::SharedInvocationUnavailable)?;
            let mut connection =
                transaction.open_planner_metadata_connection_with_state(std::mem::take(&mut state));
            let result = dispatch_planner_request(ServiceRole::Planner, &mut connection, &request);
            state = connection.detach();
            result
        };
        let response = match response {
            Ok(response) => response,
            Err(error) => {
                abort_shared_planner_invocation(invocation)?;
                return Err(PlannerConnectionError::Request(error));
            }
        };
        if let Err(error) = crate::ipc::write_frame(stream, &response) {
            abort_shared_planner_invocation(invocation)?;
            return Err(PlannerConnectionError::Frame(error));
        }
    }
}

fn abort_shared_planner_invocation(
    invocation: &Arc<Mutex<Option<crate::transaction::InvocationTransaction<'_>>>>,
) -> Result<(), PlannerConnectionError> {
    let mut invocation = match invocation.lock() {
        Ok(invocation) => invocation,
        Err(poisoned) => {
            let mut invocation = poisoned.into_inner();
            if let Some(mut transaction) = invocation.take() {
                let _ = transaction.abort();
            }
            return Err(PlannerConnectionError::SharedInvocationPoisoned);
        }
    };
    if let Some(transaction) = invocation.as_mut() {
        transaction.abort().map_err(PlannerConnectionError::Abort)?;
    }
    Ok(())
}

#[cfg(test)]
mod scan_estimate_request_tests {
    use super::{DescribePlanRelationRequest, EstimatePredicateRequest, EstimatePredicateResponse};

    #[test]
    fn scan_estimate_request_is_opt_in_and_canonical() {
        let request = DescribePlanRelationRequest {
            schema_name: "public".to_owned(),
            relation_name: "items".to_owned(),
            include_scan_estimate: false,
            population_estimate_max_rows: None,
        };
        assert_eq!(
            DescribePlanRelationRequest::decode(&request.encode().unwrap()).unwrap(),
            request
        );
        let opted_in = DescribePlanRelationRequest {
            include_scan_estimate: true,
            ..request
        };
        let encoded = opted_in.encode().unwrap();
        assert_eq!(&encoded[encoded.len() - 3..], &[0x52, 0x03, 0x01]);
        assert_eq!(
            DescribePlanRelationRequest::decode(&encoded).unwrap(),
            opted_in
        );
        let mut malformed = encoded;
        *malformed.last_mut().unwrap() = 0;
        assert!(DescribePlanRelationRequest::decode(&malformed).is_err());

        let join_estimate = DescribePlanRelationRequest {
            include_scan_estimate: false,
            population_estimate_max_rows: Some(1024),
            ..opted_in
        };
        let encoded = join_estimate.encode().unwrap();
        assert_eq!(&encoded[encoded.len() - 4..], &[0x52, 0x07, 4, 0]);
        assert_eq!(
            DescribePlanRelationRequest::decode(&encoded).unwrap(),
            join_estimate
        );
        let mut over_budget = encoded;
        let budget_offset = over_budget.len() - 2;
        over_budget[budget_offset..].copy_from_slice(&1025u16.to_be_bytes());
        assert!(DescribePlanRelationRequest::decode(&over_budget).is_err());
        assert!(
            DescribePlanRelationRequest {
                population_estimate_max_rows: Some(1025),
                ..join_estimate.clone()
            }
            .encode()
            .is_err()
        );
        assert!(
            DescribePlanRelationRequest {
                include_scan_estimate: true,
                population_estimate_max_rows: Some(1024),
                ..join_estimate
            }
            .encode()
            .is_err()
        );
    }

    #[test]
    fn predicate_estimate_request_and_response_are_bounded_and_canonical() {
        let column = crate::optimizer_binding::BoundColumn {
            column_id: [7; 16],
            ordinal: 0,
            scalar_type: crate::tuple::ScalarType::Integer,
            nullable: true,
        };
        let predicate = crate::optimizer_binding::BoundPredicate::Compare {
            operator: crate::optimizer_request::Comparison::Equal,
            left: Box::new(crate::optimizer_binding::BoundPredicate::Column(
                column.clone(),
            )),
            right: Box::new(crate::optimizer_binding::BoundPredicate::Integer(7)),
        };
        let request = EstimatePredicateRequest::from_bound_predicate(
            "public".to_owned(),
            "items".to_owned(),
            &predicate,
        )
        .unwrap();
        let encoded = request.encode().unwrap();
        let decoded = EstimatePredicateRequest::decode(&encoded).unwrap();
        assert_eq!(decoded, request);
        assert_eq!(
            crate::optimizer_plan::decode_bound_predicate_v1(&decoded.predicate_bytes, &[column],)
                .unwrap(),
            predicate
        );
        let date_column = crate::optimizer_binding::BoundColumn {
            column_id: [8; 16],
            ordinal: 1,
            scalar_type: crate::tuple::ScalarType::Date,
            nullable: false,
        };
        let date_predicate = crate::optimizer_binding::BoundPredicate::Compare {
            operator: crate::optimizer_request::Comparison::GreaterOrEqual,
            left: Box::new(crate::optimizer_binding::BoundPredicate::Column(
                date_column.clone(),
            )),
            right: Box::new(crate::optimizer_binding::BoundPredicate::Date(-42)),
        };
        let date_request = EstimatePredicateRequest::from_bound_predicate(
            "public".to_owned(),
            "items".to_owned(),
            &date_predicate,
        )
        .unwrap();
        let date_decoded =
            EstimatePredicateRequest::decode(&date_request.encode().unwrap()).unwrap();
        assert_eq!(date_decoded, date_request);
        assert_eq!(
            crate::optimizer_plan::decode_bound_predicate_v1(
                &date_decoded.predicate_bytes,
                &[date_column],
            )
            .unwrap(),
            date_predicate
        );
        let uuid_column = crate::optimizer_binding::BoundColumn {
            column_id: [9; 16],
            ordinal: 2,
            scalar_type: crate::tuple::ScalarType::Uuid,
            nullable: false,
        };
        let uuid_predicate = crate::optimizer_binding::BoundPredicate::Compare {
            operator: crate::optimizer_request::Comparison::Equal,
            left: Box::new(crate::optimizer_binding::BoundPredicate::Column(
                uuid_column.clone(),
            )),
            right: Box::new(crate::optimizer_binding::BoundPredicate::Uuid([0x12; 16])),
        };
        let uuid_request = EstimatePredicateRequest::from_bound_predicate(
            "public".to_owned(),
            "items".to_owned(),
            &uuid_predicate,
        )
        .unwrap();
        let uuid_decoded =
            EstimatePredicateRequest::decode(&uuid_request.encode().unwrap()).unwrap();
        assert_eq!(uuid_decoded, uuid_request);
        assert_eq!(
            crate::optimizer_plan::decode_bound_predicate_v1(
                &uuid_decoded.predicate_bytes,
                &[uuid_column],
            )
            .unwrap(),
            uuid_predicate
        );
        let timestamp_column = crate::optimizer_binding::BoundColumn {
            column_id: [10; 16],
            ordinal: 3,
            scalar_type: crate::tuple::ScalarType::TimestampTz,
            nullable: false,
        };
        let timestamp_predicate = crate::optimizer_binding::BoundPredicate::Compare {
            operator: crate::optimizer_request::Comparison::Equal,
            left: Box::new(crate::optimizer_binding::BoundPredicate::Column(
                timestamp_column.clone(),
            )),
            right: Box::new(crate::optimizer_binding::BoundPredicate::TimestampTz(-42)),
        };
        let timestamp_request = EstimatePredicateRequest::from_bound_predicate(
            "public".to_owned(),
            "items".to_owned(),
            &timestamp_predicate,
        )
        .unwrap();
        let timestamp_decoded =
            EstimatePredicateRequest::decode(&timestamp_request.encode().unwrap()).unwrap();
        assert_eq!(timestamp_decoded, timestamp_request);
        assert_eq!(
            crate::optimizer_plan::decode_bound_predicate_v1(
                &timestamp_decoded.predicate_bytes,
                &[timestamp_column],
            )
            .unwrap(),
            timestamp_predicate
        );
        let bytea_column = crate::optimizer_binding::BoundColumn {
            column_id: [11; 16],
            ordinal: 4,
            scalar_type: crate::tuple::ScalarType::Bytea,
            nullable: false,
        };
        let bytea_predicate = crate::optimizer_binding::BoundPredicate::Compare {
            operator: crate::optimizer_request::Comparison::Equal,
            left: Box::new(crate::optimizer_binding::BoundPredicate::Column(
                bytea_column.clone(),
            )),
            right: Box::new(crate::optimizer_binding::BoundPredicate::Bytea(vec![
                0, 0x80, 0xff,
            ])),
        };
        let bytea_request = EstimatePredicateRequest::from_bound_predicate(
            "public".to_owned(),
            "items".to_owned(),
            &bytea_predicate,
        )
        .unwrap();
        let bytea_decoded =
            EstimatePredicateRequest::decode(&bytea_request.encode().unwrap()).unwrap();
        assert_eq!(bytea_decoded, bytea_request);
        assert_eq!(
            crate::optimizer_plan::decode_bound_predicate_v1(
                &bytea_decoded.predicate_bytes,
                &[bytea_column],
            )
            .unwrap(),
            bytea_predicate
        );
        let decimal_column = crate::optimizer_binding::BoundColumn {
            column_id: [12; 16],
            ordinal: 5,
            scalar_type: crate::tuple::ScalarType::Decimal {
                precision: 8,
                scale: 2,
            },
            nullable: false,
        };
        let decimal_predicate = crate::optimizer_binding::BoundPredicate::Compare {
            operator: crate::optimizer_request::Comparison::Greater,
            left: Box::new(crate::optimizer_binding::BoundPredicate::Column(
                decimal_column.clone(),
            )),
            right: Box::new(crate::optimizer_binding::BoundPredicate::Decimal(12_345)),
        };
        let decimal_request = EstimatePredicateRequest::from_bound_predicate(
            "public".to_owned(),
            "items".to_owned(),
            &decimal_predicate,
        )
        .unwrap();
        let decimal_decoded =
            EstimatePredicateRequest::decode(&decimal_request.encode().unwrap()).unwrap();
        assert_eq!(decimal_decoded, decimal_request);
        assert_eq!(
            crate::optimizer_plan::decode_bound_predicate_v1(
                &decimal_decoded.predicate_bytes,
                &[decimal_column],
            )
            .unwrap(),
            decimal_predicate
        );
        for response in [
            EstimatePredicateResponse::Exact {
                visible_rows: 4,
                matching_rows: 1,
            },
            EstimatePredicateResponse::Exact {
                visible_rows: 0,
                matching_rows: 0,
            },
            EstimatePredicateResponse::Unknown,
        ] {
            assert_eq!(
                EstimatePredicateResponse::decode(&response.encode().unwrap()).unwrap(),
                response
            );
        }
        assert!(
            EstimatePredicateResponse::Exact {
                visible_rows: 1,
                matching_rows: 2,
            }
            .encode()
            .is_err()
        );
        let mut malformed = encoded;
        *malformed.last_mut().unwrap() ^= 1;
        assert!(EstimatePredicateRequest::decode(&malformed).is_err());
    }
}

#[cfg(test)]
mod join_estimate_request_tests {
    use super::{EstimateJoinRequest, EstimateJoinResponse};

    #[test]
    fn join_cardinality_estimate_codec_is_bounded_and_canonical() {
        let request = EstimateJoinRequest {
            schema_name: "sales".to_owned(),
            left_relation_name: "orders".to_owned(),
            right_relation_name: "items".to_owned(),
            left_key_column_ids: vec![[0x11; 16], [0x12; 16]],
            right_key_column_ids: vec![[0x21; 16], [0x22; 16]],
            maximum_rows: 1024,
        };
        let encoded = request.encode().unwrap();
        assert_eq!(&encoded[..2], &[0x52, 0x08]);
        assert_eq!(EstimateJoinRequest::decode(&encoded).unwrap(), request);

        for response in [
            EstimateJoinResponse::Exact { joined_rows: 0 },
            EstimateJoinResponse::Exact {
                joined_rows: 16_384,
            },
            EstimateJoinResponse::Unknown,
        ] {
            assert_eq!(
                EstimateJoinResponse::decode(&response.encode().unwrap()).unwrap(),
                response
            );
        }

        let mut malformed = encoded.clone();
        malformed[2..4].copy_from_slice(&1025u16.to_be_bytes());
        assert!(EstimateJoinRequest::decode(&malformed).is_err());
        let mut mismatched_keys = request.clone();
        mismatched_keys.right_key_column_ids.pop();
        assert!(mismatched_keys.encode().is_err());
        let mut zero_key = request;
        zero_key.left_key_column_ids[0] = [0; 16];
        assert!(zero_key.encode().is_err());
    }
}
