//! Bounded local IPC frame primitives.

use std::io::{Read, Write};

const MAGIC: &[u8; 4] = b"PRDI";
pub const FRAME_VERSION: u16 = 1;
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const HEADER_LEN: usize = 20;

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
    BootstrapDeliverCredential = 0x0101,
    BootstrapAcknowledgeDelivery = 0x0102,
    ResolveRelation = 0x1101,
    OpenScan = 0x1102,
    NextBatch = 0x1103,
    CloseCursor = 0x1104,
    StageInsert = 0x1110,
    StageUpdate = 0x1111,
    StageDelete = 0x1112,
    DescribeRelation = 0x1201,
    EstimateScan = 0x1202,
    FinalizeTransaction = 0x1301,
    AbortTransaction = 0x1302,
    SupervisorCancelInvocation = 0x1303,
    SupervisorGetInvocationStatus = 0x1304,
    AuthenticateSession = 0x1401,
    CloseSession = 0x1402,
    PrepareSubmission = 0x1403,
    UploadSubmissionChunk = 0x1404,
    SealSubmission = 0x1405,
    StartSubmission = 0x1406,
    ClientGetInvocationStatus = 0x1407,
    ClientCancelInvocation = 0x1408,
    SubmitAdministrativeOperation = 0x1409,
    CommitAdministrativeDdl = 0x1501,
    IssueCredential = 0x1502,
    RevokeCredential = 0x1503,
    ListCredentials = 0x1504,
    AdminCancelInvocation = 0x1505,
}

impl Operation {
    pub fn from_code(code: u16) -> Result<Self, DispatchError> {
        match code {
            0x0101 => Ok(Self::BootstrapDeliverCredential),
            0x0102 => Ok(Self::BootstrapAcknowledgeDelivery),
            0x1101 => Ok(Self::ResolveRelation),
            0x1102 => Ok(Self::OpenScan),
            0x1103 => Ok(Self::NextBatch),
            0x1104 => Ok(Self::CloseCursor),
            0x1110 => Ok(Self::StageInsert),
            0x1111 => Ok(Self::StageUpdate),
            0x1112 => Ok(Self::StageDelete),
            0x1201 => Ok(Self::DescribeRelation),
            0x1202 => Ok(Self::EstimateScan),
            0x1301 => Ok(Self::FinalizeTransaction),
            0x1302 => Ok(Self::AbortTransaction),
            0x1303 => Ok(Self::SupervisorCancelInvocation),
            0x1304 => Ok(Self::SupervisorGetInvocationStatus),
            0x1401 => Ok(Self::AuthenticateSession),
            0x1402 => Ok(Self::CloseSession),
            0x1403 => Ok(Self::PrepareSubmission),
            0x1404 => Ok(Self::UploadSubmissionChunk),
            0x1405 => Ok(Self::SealSubmission),
            0x1406 => Ok(Self::StartSubmission),
            0x1407 => Ok(Self::ClientGetInvocationStatus),
            0x1408 => Ok(Self::ClientCancelInvocation),
            0x1409 => Ok(Self::SubmitAdministrativeOperation),
            0x1501 => Ok(Self::CommitAdministrativeDdl),
            0x1502 => Ok(Self::IssueCredential),
            0x1503 => Ok(Self::RevokeCredential),
            0x1504 => Ok(Self::ListCredentials),
            0x1505 => Ok(Self::AdminCancelInvocation),
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

impl std::fmt::Display for FrameError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "IPC frame error: {self:?}")
    }
}

impl std::error::Error for FrameError {}

pub fn encode_frame(frame: &IpcFrame) -> Result<Vec<u8>, FrameError> {
    validate_operation_and_request(frame.operation, frame.request_id)?;
    if frame.payload.len() > MAX_PAYLOAD_BYTES {
        return Err(FrameError::PayloadTooLarge);
    }
    let payload_len =
        u32::try_from(frame.payload.len()).map_err(|_| FrameError::PayloadTooLarge)?;
    let mut encoded = Vec::with_capacity(HEADER_LEN + frame.payload.len());
    encoded.extend_from_slice(MAGIC);
    encoded.extend_from_slice(&FRAME_VERSION.to_be_bytes());
    encoded.extend_from_slice(&frame.operation.to_be_bytes());
    encoded.extend_from_slice(&frame.request_id.to_be_bytes());
    encoded.extend_from_slice(&payload_len.to_be_bytes());
    encoded.extend_from_slice(&frame.payload);
    Ok(encoded)
}

pub fn write_frame<W: Write>(writer: &mut W, frame: &IpcFrame) -> Result<(), FrameError> {
    validate_operation_and_request(frame.operation, frame.request_id)?;
    if frame.payload.len() > MAX_PAYLOAD_BYTES {
        return Err(FrameError::PayloadTooLarge);
    }
    let payload_len =
        u32::try_from(frame.payload.len()).map_err(|_| FrameError::PayloadTooLarge)?;
    let mut header = [0; HEADER_LEN];
    header[..4].copy_from_slice(MAGIC);
    header[4..6].copy_from_slice(&FRAME_VERSION.to_be_bytes());
    header[6..8].copy_from_slice(&frame.operation.to_be_bytes());
    header[8..16].copy_from_slice(&frame.request_id.to_be_bytes());
    header[16..20].copy_from_slice(&payload_len.to_be_bytes());
    writer.write_all(&header).map_err(FrameError::Io)?;
    writer.write_all(&frame.payload).map_err(FrameError::Io)
}

pub fn read_frame<R: Read>(reader: &mut R) -> Result<Option<IpcFrame>, FrameError> {
    let mut header = [0u8; HEADER_LEN];
    loop {
        match reader.read(&mut header[..1]) {
            Ok(0) => return Ok(None),
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(FrameError::Io(error)),
        }
    }
    read_exact(reader, &mut header[1..])?;
    if &header[..4] != MAGIC {
        return Err(FrameError::InvalidMagic);
    }
    let version = u16::from_be_bytes(header[4..6].try_into().expect("fixed header range"));
    if version != FRAME_VERSION {
        return Err(FrameError::UnsupportedVersion(version));
    }
    let operation = u16::from_be_bytes(header[6..8].try_into().expect("fixed header range"));
    let request_id = u64::from_be_bytes(header[8..16].try_into().expect("fixed header range"));
    validate_operation_and_request(operation, request_id)?;
    let payload_len = u32::from_be_bytes(header[16..20].try_into().expect("fixed header range"));
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
