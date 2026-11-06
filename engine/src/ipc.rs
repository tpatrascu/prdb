//! Bounded local IPC frame primitives.

use std::io::{Read, Write};

const MAGIC: &[u8; 4] = b"PRDI";
pub const FRAME_VERSION: u16 = 1;
pub const MAX_PAYLOAD_BYTES: usize = 1024 * 1024;
pub const HEADER_LEN: usize = 20;

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
