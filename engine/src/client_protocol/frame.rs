use super::json::*;
use super::types::*;
use super::*;
use std::collections::HashMap;
use zeroize::Zeroize;

/// Transaction-control requests carried by protocol v2.0 frames.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientV2Request {
    TxBegin {
        begin_request_id: [u8; 16],
    },
    TxAttach {
        transaction_id: [u8; 16],
    },
    TxCommit {
        transaction_id: [u8; 16],
    },
    TxRollback {
        transaction_id: [u8; 16],
    },
    TxStatus {
        transaction_id: [u8; 16],
    },
    PackageStatus {
        transaction_id: [u8; 16],
        package_id: [u8; 16],
    },
}

impl ClientV2Request {
    /// Decode the strict JSON payload for one v2 transaction-control frame.
    pub fn decode(kind: u16, payload: &[u8]) -> Result<Self, ClientControlError> {
        if payload.len() > CLIENT_MAX_CONTROL_BYTES {
            return Err(ClientControlError::InvalidJson);
        }
        let mut fields = parse_string_object(payload)?;
        let request = match kind {
            CLIENT_V2_TX_BEGIN_KIND => Self::TxBegin {
                begin_request_id: decode_lower_hex_id(&take_required(
                    &mut fields,
                    "begin_request_id",
                )?)?,
            },
            CLIENT_V2_TX_ATTACH_KIND => Self::TxAttach {
                transaction_id: decode_lower_hex_id(&take_required(
                    &mut fields,
                    "transaction_id",
                )?)?,
            },
            CLIENT_V2_TX_COMMIT_KIND => Self::TxCommit {
                transaction_id: decode_lower_hex_id(&take_required(
                    &mut fields,
                    "transaction_id",
                )?)?,
            },
            CLIENT_V2_TX_ROLLBACK_KIND => Self::TxRollback {
                transaction_id: decode_lower_hex_id(&take_required(
                    &mut fields,
                    "transaction_id",
                )?)?,
            },
            CLIENT_V2_TX_STATUS_KIND => Self::TxStatus {
                transaction_id: decode_lower_hex_id(&take_required(
                    &mut fields,
                    "transaction_id",
                )?)?,
            },
            CLIENT_V2_PACKAGE_STATUS_KIND => Self::PackageStatus {
                transaction_id: decode_lower_hex_id(&take_required(
                    &mut fields,
                    "transaction_id",
                )?)?,
                package_id: decode_lower_hex_id(&take_required(&mut fields, "package_id")?)?,
            },
            _ => return Err(ClientControlError::UnsupportedV2Operation),
        };
        reject_unknown_fields(fields)?;
        Ok(request)
    }

    /// Encode this request using the canonical v2 JSON field order and hex IDs.
    pub fn encode(self) -> Result<(u16, Vec<u8>), ClientControlError> {
        let (kind, payload) = match self {
            Self::TxBegin { begin_request_id } => (
                CLIENT_V2_TX_BEGIN_KIND,
                format!(
                    "{{\"begin_request_id\":\"{}\"}}",
                    encode_lower_hex_id(begin_request_id)
                ),
            ),
            Self::TxAttach { transaction_id } => (
                CLIENT_V2_TX_ATTACH_KIND,
                format!(
                    "{{\"transaction_id\":\"{}\"}}",
                    encode_lower_hex_id(transaction_id)
                ),
            ),
            Self::TxCommit { transaction_id } => (
                CLIENT_V2_TX_COMMIT_KIND,
                format!(
                    "{{\"transaction_id\":\"{}\"}}",
                    encode_lower_hex_id(transaction_id)
                ),
            ),
            Self::TxRollback { transaction_id } => (
                CLIENT_V2_TX_ROLLBACK_KIND,
                format!(
                    "{{\"transaction_id\":\"{}\"}}",
                    encode_lower_hex_id(transaction_id)
                ),
            ),
            Self::TxStatus { transaction_id } => (
                CLIENT_V2_TX_STATUS_KIND,
                format!(
                    "{{\"transaction_id\":\"{}\"}}",
                    encode_lower_hex_id(transaction_id)
                ),
            ),
            Self::PackageStatus {
                transaction_id,
                package_id,
            } => (
                CLIENT_V2_PACKAGE_STATUS_KIND,
                format!(
                    "{{\"transaction_id\":\"{}\",\"package_id\":\"{}\"}}",
                    encode_lower_hex_id(transaction_id),
                    encode_lower_hex_id(package_id)
                ),
            ),
        };
        Ok((kind, payload.into_bytes()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientV2TransactionState {
    Active,
    AbortOnly,
    Committing,
    Committed,
    Aborted,
    Expired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientV2PackageState {
    Running,
    Completed,
    Failed,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ClientV2Response {
    TransactionBegun {
        transaction_id: [u8; 16],
        state: ClientV2TransactionState,
        idle_timeout_ms: u64,
        lifetime_ms: u64,
        max_package_bytes: u64,
        max_packages: u64,
    },
    Transaction {
        transaction_id: [u8; 16],
        state: ClientV2TransactionState,
    },
    Package {
        transaction_id: [u8; 16],
        package_id: [u8; 16],
        state: ClientV2PackageState,
        result_lost: bool,
    },
}

impl ClientV2Response {
    /// Decode the strict response schema associated with a v2 request kind.
    pub fn decode(request_kind: u16, payload: &[u8]) -> Result<Self, ClientControlError> {
        let mut fields = parse_scalar_object(payload)?;
        let response = match request_kind {
            CLIENT_V2_TX_BEGIN_KIND => {
                let transaction_id =
                    decode_lower_hex_id(&take_string(&mut fields, "transaction_id")?)?;
                let state = decode_tx_state(&take_string(&mut fields, "state")?)?;
                let idle_timeout_ms = take_number(&mut fields, "idle_timeout_ms")?;
                let lifetime_ms = take_number(&mut fields, "lifetime_ms")?;
                let max_package_bytes = take_number(&mut fields, "max_package_bytes")?;
                let max_packages = take_number(&mut fields, "max_packages")?;
                if idle_timeout_ms == 0
                    || lifetime_ms == 0
                    || max_package_bytes == 0
                    || max_packages == 0
                {
                    return Err(ClientControlError::InvalidPrepare);
                }
                Self::TransactionBegun {
                    transaction_id,
                    state,
                    idle_timeout_ms,
                    lifetime_ms,
                    max_package_bytes,
                    max_packages,
                }
            }
            CLIENT_V2_TX_ATTACH_KIND
            | CLIENT_V2_TX_COMMIT_KIND
            | CLIENT_V2_TX_ROLLBACK_KIND
            | CLIENT_V2_TX_STATUS_KIND => {
                let transaction_id =
                    decode_lower_hex_id(&take_string(&mut fields, "transaction_id")?)?;
                let state = decode_tx_state(&take_string(&mut fields, "state")?)?;
                if request_kind == CLIENT_V2_TX_ATTACH_KIND
                    && !matches!(
                        state,
                        ClientV2TransactionState::Active | ClientV2TransactionState::AbortOnly
                    )
                    || request_kind == CLIENT_V2_TX_COMMIT_KIND
                        && !matches!(
                            state,
                            ClientV2TransactionState::Committed
                                | ClientV2TransactionState::Aborted
                                | ClientV2TransactionState::Expired
                        )
                    || request_kind == CLIENT_V2_TX_ROLLBACK_KIND
                        && !matches!(
                            state,
                            ClientV2TransactionState::Aborted | ClientV2TransactionState::Expired
                        )
                {
                    return Err(ClientControlError::InvalidPrepare);
                }
                Self::Transaction {
                    transaction_id,
                    state,
                }
            }
            CLIENT_V2_PACKAGE_STATUS_KIND => {
                let transaction_id =
                    decode_lower_hex_id(&take_string(&mut fields, "transaction_id")?)?;
                let package_id = decode_lower_hex_id(&take_string(&mut fields, "package_id")?)?;
                let state = decode_package_state(&take_string(&mut fields, "state")?)?;
                let result_lost = take_bool(&mut fields, "result_lost")?;
                if result_lost && state != ClientV2PackageState::Completed {
                    return Err(ClientControlError::InvalidPrepare);
                }
                Self::Package {
                    transaction_id,
                    package_id,
                    state,
                    result_lost,
                }
            }
            _ => return Err(ClientControlError::UnsupportedV2Operation),
        };
        reject_unknown_scalars(fields)?;
        Ok(response)
    }

    /// Encode this response as canonical JSON payload for a kind-100 frame.
    pub fn encode(&self) -> Vec<u8> {
        let payload = match self {
            Self::TransactionBegun {
                transaction_id,
                state,
                idle_timeout_ms,
                lifetime_ms,
                max_package_bytes,
                max_packages,
            } => format!(
                "{{\"transaction_id\":\"{}\",\"state\":\"{}\",\"idle_timeout_ms\":{idle_timeout_ms},\"lifetime_ms\":{lifetime_ms},\"max_package_bytes\":{max_package_bytes},\"max_packages\":{max_packages}}}",
                encode_lower_hex_id(*transaction_id),
                encode_tx_state(*state)
            ),
            Self::Transaction {
                transaction_id,
                state,
            } => format!(
                "{{\"transaction_id\":\"{}\",\"state\":\"{}\"}}",
                encode_lower_hex_id(*transaction_id),
                encode_tx_state(*state)
            ),
            Self::Package {
                transaction_id,
                package_id,
                state,
                result_lost,
            } => format!(
                "{{\"transaction_id\":\"{}\",\"package_id\":\"{}\",\"state\":\"{}\",\"result_lost\":{result_lost}}}",
                encode_lower_hex_id(*transaction_id),
                encode_lower_hex_id(*package_id),
                encode_package_state(*state)
            ),
        };
        payload.into_bytes()
    }
}

pub(super) fn decode_tx_state(text: &str) -> Result<ClientV2TransactionState, ClientControlError> {
    match text {
        "active" => Ok(ClientV2TransactionState::Active),
        "abort_only" => Ok(ClientV2TransactionState::AbortOnly),
        "committing" => Ok(ClientV2TransactionState::Committing),
        "committed" => Ok(ClientV2TransactionState::Committed),
        "aborted" => Ok(ClientV2TransactionState::Aborted),
        "expired" => Ok(ClientV2TransactionState::Expired),
        _ => Err(ClientControlError::InvalidPrepare),
    }
}

pub(super) fn encode_tx_state(state: ClientV2TransactionState) -> &'static str {
    match state {
        ClientV2TransactionState::Active => "active",
        ClientV2TransactionState::AbortOnly => "abort_only",
        ClientV2TransactionState::Committing => "committing",
        ClientV2TransactionState::Committed => "committed",
        ClientV2TransactionState::Aborted => "aborted",
        ClientV2TransactionState::Expired => "expired",
    }
}

pub(super) fn client_v2_transaction_state(
    state: crate::storage_manager::InteractiveTransactionState,
) -> ClientV2TransactionState {
    match state {
        crate::storage_manager::InteractiveTransactionState::Active => {
            ClientV2TransactionState::Active
        }
        crate::storage_manager::InteractiveTransactionState::AbortOnly => {
            ClientV2TransactionState::AbortOnly
        }
        crate::storage_manager::InteractiveTransactionState::Committing => {
            ClientV2TransactionState::Committing
        }
        crate::storage_manager::InteractiveTransactionState::Committed => {
            ClientV2TransactionState::Committed
        }
        crate::storage_manager::InteractiveTransactionState::Aborted => {
            ClientV2TransactionState::Aborted
        }
        crate::storage_manager::InteractiveTransactionState::Expired => {
            ClientV2TransactionState::Expired
        }
    }
}

pub(super) fn decode_package_state(text: &str) -> Result<ClientV2PackageState, ClientControlError> {
    match text {
        "running" => Ok(ClientV2PackageState::Running),
        "completed" => Ok(ClientV2PackageState::Completed),
        "failed" => Ok(ClientV2PackageState::Failed),
        "unknown" => Ok(ClientV2PackageState::Unknown),
        _ => Err(ClientControlError::InvalidPrepare),
    }
}

pub(super) fn encode_package_state(state: ClientV2PackageState) -> &'static str {
    match state {
        ClientV2PackageState::Running => "running",
        ClientV2PackageState::Completed => "completed",
        ClientV2PackageState::Failed => "failed",
        ClientV2PackageState::Unknown => "unknown",
    }
}

pub struct ClientFrameDecoder {
    header: [u8; CLIENT_FRAME_HEADER_BYTES],
    header_bytes: usize,
    pending_header: Option<Header>,
    pending_payload: Vec<u8>,
    protocol_major: u16,
    protocol_minor: u16,
}

impl Default for ClientFrameDecoder {
    fn default() -> Self {
        Self {
            header: [0; CLIENT_FRAME_HEADER_BYTES],
            header_bytes: 0,
            pending_header: None,
            pending_payload: Vec::new(),
            protocol_major: 1,
            protocol_minor: 0,
        }
    }
}

impl ClientFrameDecoder {
    /// Select protocol 1.0 or 2.0 from the first complete frame header, then
    /// require every later frame on this connection to use that version.
    pub fn negotiating() -> Self {
        Self {
            header: [0; CLIENT_FRAME_HEADER_BYTES],
            header_bytes: 0,
            pending_header: None,
            pending_payload: Vec::new(),
            protocol_major: 0,
            protocol_minor: 0,
        }
    }

    /// Decode the connection-independent interactive SQL protocol v2.0.
    pub fn v2() -> Self {
        Self {
            header: [0; CLIENT_FRAME_HEADER_BYTES],
            header_bytes: 0,
            pending_header: None,
            pending_payload: Vec::new(),
            protocol_major: 2,
            protocol_minor: 0,
        }
    }

    pub fn protocol_version(&self) -> Option<(u16, u16)> {
        (self.protocol_major != 0).then_some((self.protocol_major, self.protocol_minor))
    }

    /// Add any part of the TCP byte stream and return all complete frames.
    /// Header limits are checked before payload storage is reserved.
    pub fn push(&mut self, mut input: &[u8]) -> Result<Vec<ClientFrame>, ClientFrameError> {
        let mut frames = Vec::new();
        while !input.is_empty() {
            if self.pending_header.is_none() {
                let needed = CLIENT_FRAME_HEADER_BYTES - self.header_bytes;
                let copied = needed.min(input.len());
                self.header[self.header_bytes..self.header_bytes + copied]
                    .copy_from_slice(&input[..copied]);
                self.header_bytes += copied;
                input = &input[copied..];
                if self.header_bytes < CLIENT_FRAME_HEADER_BYTES {
                    continue;
                }
                let (header, major, minor) =
                    decode_header(&self.header, self.protocol_major, self.protocol_minor)?;
                if self.protocol_major == 0 {
                    self.protocol_major = major;
                    self.protocol_minor = minor;
                }
                self.pending_payload = Vec::new();
                self.pending_payload
                    .try_reserve_exact(header.payload_bytes)
                    .map_err(|_| ClientFrameError::AllocationFailed)?;
                self.pending_header = Some(header);
                if header.payload_bytes == 0 {
                    frames.push(self.take_pending_frame());
                    continue;
                }
            }

            let header = self.pending_header.expect("pending frame header");
            let needed = header.payload_bytes - self.pending_payload.len();
            let copied = needed.min(input.len());
            self.pending_payload.extend_from_slice(&input[..copied]);
            input = &input[copied..];
            if self.pending_payload.len() == header.payload_bytes {
                frames.push(self.take_pending_frame());
            }
        }
        Ok(frames)
    }

    /// Reject an EOF that leaves any partial header or payload buffered.
    pub fn finish(&self) -> Result<(), ClientFrameError> {
        if self.header_bytes != 0 || self.pending_header.is_some() {
            Err(ClientFrameError::IncompleteFrame)
        } else {
            Ok(())
        }
    }

    fn take_pending_frame(&mut self) -> ClientFrame {
        let header = self.pending_header.take().expect("pending frame header");
        let payload = std::mem::take(&mut self.pending_payload);
        self.header_bytes = 0;
        ClientFrame {
            kind: header.kind,
            request_id: header.request_id,
            response_sequence: header.response_sequence,
            payload,
        }
    }
}

impl Drop for ClientFrameDecoder {
    fn drop(&mut self) {
        self.pending_payload.zeroize();
    }
}

impl ClientFrame {
    pub fn encode(&self) -> Result<Vec<u8>, ClientFrameError> {
        self.encode_for_version(1, 0)
    }

    pub fn encode_v2(&self) -> Result<Vec<u8>, ClientFrameError> {
        self.encode_for_version(2, 0)
    }

    fn encode_for_version(
        &self,
        protocol_major: u16,
        protocol_minor: u16,
    ) -> Result<Vec<u8>, ClientFrameError> {
        let header = Header {
            kind: self.kind,
            payload_bytes: self.payload.len(),
            request_id: self.request_id,
            response_sequence: self.response_sequence,
        };
        validate_header_fields(header, protocol_major)?;
        let payload_bytes =
            u32::try_from(self.payload.len()).map_err(|_| ClientFrameError::PayloadTooLarge)?;
        let total_bytes = CLIENT_FRAME_HEADER_BYTES
            .checked_add(self.payload.len())
            .ok_or(ClientFrameError::PayloadTooLarge)?;
        let mut output = Vec::new();
        output
            .try_reserve_exact(total_bytes)
            .map_err(|_| ClientFrameError::AllocationFailed)?;
        let header = crate::query_abi::encode_client_frame_header_record(
            CLIENT_MAGIC,
            protocol_major,
            protocol_minor,
            self.kind,
            0,
            payload_bytes,
            self.request_id,
            self.response_sequence,
        );
        output.extend_from_slice(&header);
        output.extend_from_slice(&self.payload);
        Ok(output)
    }
}

pub(super) fn decode_header(
    bytes: &[u8; CLIENT_FRAME_HEADER_BYTES],
    expected_major: u16,
    expected_minor: u16,
) -> Result<(Header, u16, u16), ClientFrameError> {
    let Some((magic, major, minor, kind, flags, payload_length, request_id, response_sequence)) =
        crate::query_abi::decode_client_frame_header_record(bytes)
    else {
        return Err(ClientFrameError::InvalidMagic);
    };
    if magic != CLIENT_MAGIC {
        return Err(ClientFrameError::InvalidMagic);
    }
    let supported_negotiation = expected_major == 0 && matches!((major, minor), (1, 0) | (2, 0));
    if !supported_negotiation && (major != expected_major || minor != expected_minor) {
        return Err(ClientFrameError::UnsupportedVersion { major, minor });
    }
    if flags != 0 {
        return Err(ClientFrameError::NonzeroFlags);
    }
    let payload_bytes =
        usize::try_from(payload_length).map_err(|_| ClientFrameError::PayloadTooLarge)?;
    let header = Header {
        kind,
        payload_bytes,
        request_id,
        response_sequence,
    };
    validate_header_fields(header, major)?;
    Ok((header, major, minor))
}

pub(super) fn validate_header_fields(
    header: Header,
    protocol_major: u16,
) -> Result<(), ClientFrameError> {
    let v2 = protocol_major == 2;
    let maximum = match header.kind {
        1..=3 | 6..=11 | 100 | 101 | 103 => CLIENT_MAX_CONTROL_BYTES,
        13..=18 if v2 => CLIENT_MAX_CONTROL_BYTES,
        4 | 5 | 12 | CLIENT_SDK_LIBRARY_CHUNK_KIND | super::CLIENT_SQL_DATA_CHUNK_KIND => {
            CLIENT_MAX_CHUNK_BYTES + 24
        }
        102 => CLIENT_MAX_PAYLOAD_BYTES,
        kind => return Err(ClientFrameError::InvalidKind(kind)),
    };
    if header.payload_bytes > maximum || header.payload_bytes > CLIENT_MAX_PAYLOAD_BYTES {
        return Err(ClientFrameError::PayloadTooLarge);
    }
    if header.request_id == 0 {
        return Err(ClientFrameError::ZeroRequestId);
    }
    match header.kind {
        4 | 5 | 12 | CLIENT_SDK_LIBRARY_CHUNK_KIND | super::CLIENT_SQL_DATA_CHUNK_KIND
            if header.payload_bytes <= 24 =>
        {
            Err(ClientFrameError::InvalidChunkLength)
        }
        1..=20 if header.response_sequence != 0 => Err(ClientFrameError::InvalidSequence),
        100 | 101 if header.response_sequence != 0 => Err(ClientFrameError::InvalidSequence),
        102 | 103 if header.response_sequence == 0 => Err(ClientFrameError::InvalidSequence),
        _ => Ok(()),
    }
}

#[derive(Default)]
pub struct ClientRequestIds {
    last_request_id: u64,
}

impl ClientRequestIds {
    /// Enforce strictly increasing nonzero IDs on a single client connection.
    pub fn accept(&mut self, frame: &ClientFrame) -> Result<(), ClientFrameError> {
        if !((1..=12).contains(&frame.kind)
            || matches!(
                frame.kind,
                CLIENT_SDK_LIBRARY_CHUNK_KIND | super::CLIENT_SQL_DATA_CHUNK_KIND
            ))
            || frame.request_id == 0
            || frame.request_id <= self.last_request_id
        {
            return Err(ClientFrameError::RequestIdsNotIncreasing);
        }
        self.last_request_id = frame.request_id;
        Ok(())
    }

    /// Enforce v2 request kinds and strictly increasing nonzero IDs.
    pub fn accept_v2(&mut self, frame: &ClientFrame) -> Result<(), ClientFrameError> {
        self.accept_through(frame, 18)
    }

    fn accept_through(
        &mut self,
        frame: &ClientFrame,
        max_kind: u16,
    ) -> Result<(), ClientFrameError> {
        if !(1..=max_kind).contains(&frame.kind)
            || frame.request_id == 0
            || frame.request_id <= self.last_request_id
        {
            return Err(ClientFrameError::RequestIdsNotIncreasing);
        }
        self.last_request_id = frame.request_id;
        Ok(())
    }
}

#[derive(Default)]
pub struct ClientResponseSequences {
    next_by_request: HashMap<u64, u64>,
}

impl ClientResponseSequences {
    /// Begin streamed responses after the sequence-zero START acceptance.
    pub fn begin_stream(&mut self, request_id: u64) -> Result<(), ClientFrameError> {
        if request_id == 0
            || !self.next_by_request.is_empty()
            || self.next_by_request.contains_key(&request_id)
        {
            return Err(ClientFrameError::UnexpectedResponseSequence);
        }
        self.next_by_request.insert(request_id, 1);
        Ok(())
    }

    /// Validate consecutive RESULT_DATA frames and the terminal sequence.
    pub fn accept_stream_frame(&mut self, frame: &ClientFrame) -> Result<bool, ClientFrameError> {
        let expected = self
            .next_by_request
            .get(&frame.request_id)
            .copied()
            .ok_or(ClientFrameError::UnexpectedResponseSequence)?;
        if !matches!(frame.kind, 102 | 103) || frame.response_sequence != expected {
            return Err(ClientFrameError::UnexpectedResponseSequence);
        }
        if frame.kind == 103 {
            self.next_by_request.remove(&frame.request_id);
            Ok(true)
        } else {
            self.next_by_request.insert(
                frame.request_id,
                expected
                    .checked_add(1)
                    .ok_or(ClientFrameError::UnexpectedResponseSequence)?,
            );
            Ok(false)
        }
    }
}
