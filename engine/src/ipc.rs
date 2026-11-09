//! Bounded local IPC frame primitives.

use std::io::{Read, Write};

const MAGIC_OFFSET: usize = crate::query_abi::IPC_FRAME_MAGIC_OFFSET as usize;
const MAGIC_LEN: usize = crate::query_abi::IPC_FRAME_MAGIC_LEN as usize;
const VERSION_OFFSET: usize = crate::query_abi::IPC_FRAME_VERSION_OFFSET as usize;
const VERSION_LEN: usize = crate::query_abi::IPC_FRAME_VERSION_LEN as usize;
const OPERATION_OFFSET: usize = crate::query_abi::IPC_FRAME_OPERATION_OFFSET as usize;
const OPERATION_LEN: usize = crate::query_abi::IPC_FRAME_OPERATION_LEN as usize;
const REQUEST_ID_OFFSET: usize = crate::query_abi::IPC_FRAME_REQUEST_ID_OFFSET as usize;
const REQUEST_ID_LEN: usize = crate::query_abi::IPC_FRAME_REQUEST_ID_LEN as usize;
const PAYLOAD_LEN_OFFSET: usize = crate::query_abi::IPC_FRAME_PAYLOAD_LEN_OFFSET as usize;
const PAYLOAD_LEN_LEN: usize = crate::query_abi::IPC_FRAME_PAYLOAD_LEN_LEN as usize;
const MAGIC: u32 = crate::query_abi::IPC_FRAME_MAGIC;
pub const FRAME_VERSION: u16 = crate::query_abi::IPC_FRAME_VERSION;
pub const MAX_PAYLOAD_BYTES: usize = crate::query_abi::IPC_FRAME_MAX_PAYLOAD_BYTES as usize;
pub const HEADER_LEN: usize = crate::query_abi::IPC_FRAME_HEADER_LEN as usize;

/// Service authority assigned to a connection by trusted channel provisioning.
/// This value is never decoded from an IPC frame or request payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceRole {
    Bootstrap,
    Executor,
    Planner,
    Supervisor,
    Gateway,
    AdminControl,
}

/// Version-1 IPC operations. Their numeric tags are stable within the IPC
/// operation namespace and remain independent of payload schemas.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u16)]
pub enum Operation {
    BootstrapDeliverCredential = crate::query_abi::IPC_OP_BOOTSTRAP_DELIVER_CREDENTIAL,
    BootstrapAcknowledgeDelivery = crate::query_abi::IPC_OP_BOOTSTRAP_ACKNOWLEDGE_DELIVERY,
    ResolveRelation = crate::query_abi::IPC_OP_RESOLVE_RELATION,
    OpenScan = crate::query_abi::IPC_OP_OPEN_SCAN,
    NextBatch = crate::query_abi::IPC_OP_NEXT_BATCH,
    CloseCursor = crate::query_abi::IPC_OP_CLOSE_CURSOR,
    StageInsert = crate::query_abi::IPC_OP_STAGE_INSERT,
    StageUpdate = crate::query_abi::IPC_OP_STAGE_UPDATE,
    StageDelete = crate::query_abi::IPC_OP_STAGE_DELETE,
    DescribeRelation = crate::query_abi::IPC_OP_DESCRIBE_RELATION,
    EstimateScan = crate::query_abi::IPC_OP_ESTIMATE_SCAN,
    FinalizeTransaction = crate::query_abi::IPC_OP_FINALIZE_TRANSACTION,
    AbortTransaction = crate::query_abi::IPC_OP_ABORT_TRANSACTION,
    SupervisorCancelInvocation = crate::query_abi::IPC_OP_SUPERVISOR_CANCEL_INVOCATION,
    SupervisorGetInvocationStatus = crate::query_abi::IPC_OP_SUPERVISOR_GET_INVOCATION_STATUS,
    AuthenticateSession = crate::query_abi::IPC_OP_AUTHENTICATE_SESSION,
    CloseSession = crate::query_abi::IPC_OP_CLOSE_SESSION,
    PrepareSubmission = crate::query_abi::IPC_OP_PREPARE_SUBMISSION,
    UploadSubmissionChunk = crate::query_abi::IPC_OP_UPLOAD_SUBMISSION_CHUNK,
    SealSubmission = crate::query_abi::IPC_OP_SEAL_SUBMISSION,
    StartSubmission = crate::query_abi::IPC_OP_START_SUBMISSION,
    ClientGetInvocationStatus = crate::query_abi::IPC_OP_CLIENT_GET_INVOCATION_STATUS,
    ClientCancelInvocation = crate::query_abi::IPC_OP_CLIENT_CANCEL_INVOCATION,
    SubmitAdministrativeOperation = crate::query_abi::IPC_OP_SUBMIT_ADMINISTRATIVE_OPERATION,
    OpenClientSession = crate::query_abi::IPC_OP_OPEN_CLIENT_SESSION,
    PingClientSession = crate::query_abi::IPC_OP_PING_CLIENT_SESSION,
    GatewayResultData = crate::query_abi::IPC_OP_GATEWAY_RESULT_DATA,
    GatewayTerminal = crate::query_abi::IPC_OP_GATEWAY_TERMINAL,
    GatewayClientError = crate::query_abi::IPC_OP_GATEWAY_CLIENT_ERROR,
    CommitAdministrativeDdl = crate::query_abi::IPC_OP_COMMIT_ADMINISTRATIVE_DDL,
    IssueCredential = crate::query_abi::IPC_OP_ISSUE_CREDENTIAL,
    RevokeCredential = crate::query_abi::IPC_OP_REVOKE_CREDENTIAL,
    ListCredentials = crate::query_abi::IPC_OP_LIST_CREDENTIALS,
    AdminCancelInvocation = crate::query_abi::IPC_OP_ADMIN_CANCEL_INVOCATION,
}

impl Operation {
    pub fn from_code(code: u16) -> Result<Self, DispatchError> {
        match code {
            crate::query_abi::IPC_OP_BOOTSTRAP_DELIVER_CREDENTIAL => {
                Ok(Self::BootstrapDeliverCredential)
            }
            crate::query_abi::IPC_OP_BOOTSTRAP_ACKNOWLEDGE_DELIVERY => {
                Ok(Self::BootstrapAcknowledgeDelivery)
            }
            crate::query_abi::IPC_OP_RESOLVE_RELATION => Ok(Self::ResolveRelation),
            crate::query_abi::IPC_OP_OPEN_SCAN => Ok(Self::OpenScan),
            crate::query_abi::IPC_OP_NEXT_BATCH => Ok(Self::NextBatch),
            crate::query_abi::IPC_OP_CLOSE_CURSOR => Ok(Self::CloseCursor),
            crate::query_abi::IPC_OP_STAGE_INSERT => Ok(Self::StageInsert),
            crate::query_abi::IPC_OP_STAGE_UPDATE => Ok(Self::StageUpdate),
            crate::query_abi::IPC_OP_STAGE_DELETE => Ok(Self::StageDelete),
            crate::query_abi::IPC_OP_DESCRIBE_RELATION => Ok(Self::DescribeRelation),
            crate::query_abi::IPC_OP_ESTIMATE_SCAN => Ok(Self::EstimateScan),
            crate::query_abi::IPC_OP_FINALIZE_TRANSACTION => Ok(Self::FinalizeTransaction),
            crate::query_abi::IPC_OP_ABORT_TRANSACTION => Ok(Self::AbortTransaction),
            crate::query_abi::IPC_OP_SUPERVISOR_CANCEL_INVOCATION => {
                Ok(Self::SupervisorCancelInvocation)
            }
            crate::query_abi::IPC_OP_SUPERVISOR_GET_INVOCATION_STATUS => {
                Ok(Self::SupervisorGetInvocationStatus)
            }
            crate::query_abi::IPC_OP_AUTHENTICATE_SESSION => Ok(Self::AuthenticateSession),
            crate::query_abi::IPC_OP_CLOSE_SESSION => Ok(Self::CloseSession),
            crate::query_abi::IPC_OP_PREPARE_SUBMISSION => Ok(Self::PrepareSubmission),
            crate::query_abi::IPC_OP_UPLOAD_SUBMISSION_CHUNK => Ok(Self::UploadSubmissionChunk),
            crate::query_abi::IPC_OP_SEAL_SUBMISSION => Ok(Self::SealSubmission),
            crate::query_abi::IPC_OP_START_SUBMISSION => Ok(Self::StartSubmission),
            crate::query_abi::IPC_OP_CLIENT_GET_INVOCATION_STATUS => {
                Ok(Self::ClientGetInvocationStatus)
            }
            crate::query_abi::IPC_OP_CLIENT_CANCEL_INVOCATION => Ok(Self::ClientCancelInvocation),
            crate::query_abi::IPC_OP_SUBMIT_ADMINISTRATIVE_OPERATION => {
                Ok(Self::SubmitAdministrativeOperation)
            }
            crate::query_abi::IPC_OP_OPEN_CLIENT_SESSION => Ok(Self::OpenClientSession),
            crate::query_abi::IPC_OP_PING_CLIENT_SESSION => Ok(Self::PingClientSession),
            crate::query_abi::IPC_OP_GATEWAY_RESULT_DATA => Ok(Self::GatewayResultData),
            crate::query_abi::IPC_OP_GATEWAY_TERMINAL => Ok(Self::GatewayTerminal),
            crate::query_abi::IPC_OP_GATEWAY_CLIENT_ERROR => Ok(Self::GatewayClientError),
            crate::query_abi::IPC_OP_COMMIT_ADMINISTRATIVE_DDL => Ok(Self::CommitAdministrativeDdl),
            crate::query_abi::IPC_OP_ISSUE_CREDENTIAL => Ok(Self::IssueCredential),
            crate::query_abi::IPC_OP_REVOKE_CREDENTIAL => Ok(Self::RevokeCredential),
            crate::query_abi::IPC_OP_LIST_CREDENTIALS => Ok(Self::ListCredentials),
            crate::query_abi::IPC_OP_ADMIN_CANCEL_INVOCATION => Ok(Self::AdminCancelInvocation),
            _ => Err(DispatchError::UnknownOperation(code)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DispatchError {
    UnknownOperation(u16),
    OperationNotAllowed {
        role: ServiceRole,
        operation: Operation,
    },
}

impl std::fmt::Display for DispatchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "IPC dispatch error: {self:?}")
    }
}

impl std::error::Error for DispatchError {}

/// Checks a decoded operation against the role provisioned for its channel.
/// The caller must obtain `role` from trusted connection setup, never a frame.
pub fn authorize_operation(role: ServiceRole, operation: Operation) -> Result<(), DispatchError> {
    let allowed = match role {
        ServiceRole::Bootstrap => matches!(
            operation,
            Operation::BootstrapDeliverCredential | Operation::BootstrapAcknowledgeDelivery
        ),
        ServiceRole::Executor => matches!(
            operation,
            Operation::ResolveRelation
                | Operation::OpenScan
                | Operation::NextBatch
                | Operation::CloseCursor
                | Operation::StageInsert
                | Operation::StageUpdate
                | Operation::StageDelete
        ),
        ServiceRole::Planner => matches!(
            operation,
            Operation::DescribeRelation | Operation::EstimateScan
        ),
        ServiceRole::Supervisor => matches!(
            operation,
            Operation::FinalizeTransaction
                | Operation::AbortTransaction
                | Operation::SupervisorCancelInvocation
                | Operation::SupervisorGetInvocationStatus
        ),
        ServiceRole::Gateway => matches!(
            operation,
            Operation::AuthenticateSession
                | Operation::CloseSession
                | Operation::PrepareSubmission
                | Operation::UploadSubmissionChunk
                | Operation::SealSubmission
                | Operation::StartSubmission
                | Operation::ClientGetInvocationStatus
                | Operation::ClientCancelInvocation
                | Operation::OpenClientSession
                | Operation::PingClientSession
                | Operation::SubmitAdministrativeOperation
        ),
        ServiceRole::AdminControl => matches!(
            operation,
            Operation::CommitAdministrativeDdl
                | Operation::IssueCredential
                | Operation::RevokeCredential
                | Operation::ListCredentials
                | Operation::AdminCancelInvocation
        ),
    };
    if allowed {
        Ok(())
    } else {
        Err(DispatchError::OperationNotAllowed { role, operation })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IpcFrame {
    pub operation: u16,
    pub request_id: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
pub enum FrameError {
    InvalidMagic,
    UnsupportedVersion(u16),
    InvalidOperation,
    UnknownOperation(u16),
    InvalidRequestId,
    PayloadTooLarge,
    TruncatedFrame,
    Io(std::io::Error),
}

impl FrameError {
    /// Returns the shared v1 status for malformed-frame errors. I/O failures
    /// are transport errors and do not have a wire validation status.
    pub fn status_code(&self) -> Option<u32> {
        match self {
            Self::InvalidMagic => Some(crate::query_abi::IPC_FRAME_STATUS_INVALID_MAGIC),
            Self::UnsupportedVersion(_) => {
                Some(crate::query_abi::IPC_FRAME_STATUS_UNSUPPORTED_VERSION)
            }
            Self::InvalidOperation => Some(crate::query_abi::IPC_FRAME_STATUS_INVALID_OPERATION),
            Self::UnknownOperation(_) => Some(crate::query_abi::IPC_FRAME_STATUS_UNKNOWN_OPERATION),
            Self::InvalidRequestId => Some(crate::query_abi::IPC_FRAME_STATUS_INVALID_REQUEST_ID),
            Self::PayloadTooLarge => Some(crate::query_abi::IPC_FRAME_STATUS_PAYLOAD_TOO_LARGE),
            Self::TruncatedFrame => Some(crate::query_abi::IPC_FRAME_STATUS_TRUNCATED),
            Self::Io(_) => None,
        }
    }
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "IPC frame error: {self:?}")
    }
}

impl std::error::Error for FrameError {}

pub fn encode_frame(frame: &IpcFrame) -> Result<Vec<u8>, FrameError> {
    let payload_len = validate_frame(frame)?;
    let header = encode_header(frame, payload_len);
    let mut encoded = Vec::with_capacity(HEADER_LEN + frame.payload.len());
    encoded.extend_from_slice(&header);
    encoded.extend_from_slice(&frame.payload);
    Ok(encoded)
}

pub fn write_frame<W: Write>(writer: &mut W, frame: &IpcFrame) -> Result<(), FrameError> {
    let payload_len = validate_frame(frame)?;
    let header = encode_header(frame, payload_len);
    writer.write_all(&header).map_err(FrameError::Io)?;
    writer.write_all(&frame.payload).map_err(FrameError::Io)
}

fn validate_frame(frame: &IpcFrame) -> Result<u32, FrameError> {
    validate_operation_and_request(frame.operation, frame.request_id)?;
    if frame.payload.len() > MAX_PAYLOAD_BYTES {
        return Err(FrameError::PayloadTooLarge);
    }
    u32::try_from(frame.payload.len()).map_err(|_| FrameError::PayloadTooLarge)
}

fn encode_header(frame: &IpcFrame, payload_len: u32) -> [u8; HEADER_LEN] {
    let mut header = [0; HEADER_LEN];
    header[MAGIC_OFFSET..MAGIC_OFFSET + MAGIC_LEN].copy_from_slice(&MAGIC.to_be_bytes());
    header[VERSION_OFFSET..VERSION_OFFSET + VERSION_LEN]
        .copy_from_slice(&FRAME_VERSION.to_be_bytes());
    header[OPERATION_OFFSET..OPERATION_OFFSET + OPERATION_LEN]
        .copy_from_slice(&frame.operation.to_be_bytes());
    header[REQUEST_ID_OFFSET..REQUEST_ID_OFFSET + REQUEST_ID_LEN]
        .copy_from_slice(&frame.request_id.to_be_bytes());
    header[PAYLOAD_LEN_OFFSET..PAYLOAD_LEN_OFFSET + PAYLOAD_LEN_LEN]
        .copy_from_slice(&payload_len.to_be_bytes());
    header
}

pub fn read_frame<R: Read>(reader: &mut R) -> Result<Option<IpcFrame>, FrameError> {
    let mut header = [0u8; HEADER_LEN];
    loop {
        match reader.read(&mut header[MAGIC_OFFSET..MAGIC_OFFSET + 1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(FrameError::Io(error)),
        }
    }
    read_exact(reader, &mut header[MAGIC_OFFSET + 1..])?;
    let magic = u32::from_be_bytes(
        header[MAGIC_OFFSET..MAGIC_OFFSET + MAGIC_LEN]
            .try_into()
            .expect("fixed magic range"),
    );
    if magic != MAGIC {
        return Err(FrameError::InvalidMagic);
    }
    let version = u16::from_be_bytes(
        header[VERSION_OFFSET..VERSION_OFFSET + VERSION_LEN]
            .try_into()
            .expect("fixed version range"),
    );
    if version != FRAME_VERSION {
        return Err(FrameError::UnsupportedVersion(version));
    }
    let operation = u16::from_be_bytes(
        header[OPERATION_OFFSET..OPERATION_OFFSET + OPERATION_LEN]
            .try_into()
            .expect("fixed operation range"),
    );
    let request_id = u64::from_be_bytes(
        header[REQUEST_ID_OFFSET..REQUEST_ID_OFFSET + REQUEST_ID_LEN]
            .try_into()
            .expect("fixed request ID range"),
    );
    validate_operation_and_request(operation, request_id)?;
    let payload_len = u32::from_be_bytes(
        header[PAYLOAD_LEN_OFFSET..PAYLOAD_LEN_OFFSET + PAYLOAD_LEN_LEN]
            .try_into()
            .expect("fixed payload length range"),
    );
    let payload_len = usize::try_from(payload_len).map_err(|_| FrameError::PayloadTooLarge)?;
    if payload_len > MAX_PAYLOAD_BYTES {
        return Err(FrameError::PayloadTooLarge);
    }
    let mut payload = vec![0; payload_len];
    read_exact(reader, &mut payload)?;
    Ok(Some(IpcFrame {
        operation,
        request_id,
        payload,
    }))
}

fn validate_operation_and_request(operation: u16, request_id: u64) -> Result<(), FrameError> {
    if operation == 0 {
        return Err(FrameError::InvalidOperation);
    }
    if Operation::from_code(operation).is_err() {
        return Err(FrameError::UnknownOperation(operation));
    }
    if request_id == 0 {
        return Err(FrameError::InvalidRequestId);
    }
    Ok(())
}

fn read_exact(reader: &mut impl Read, bytes: &mut [u8]) -> Result<(), FrameError> {
    reader.read_exact(bytes).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            FrameError::TruncatedFrame
        } else {
            FrameError::Io(error)
        }
    })
}
