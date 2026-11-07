//! Bounded in-memory submission staging for the trusted supervisor.
//!
//! Staging does not open a storage snapshot or load native code. The caller
//! must bind `StagingOwner` to an already authenticated query connection.

use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_PARAMETER_BYTES: u64 = 1024 * 1024;
pub const MAX_MANIFEST_BYTES: usize = 16 * 1024;
pub const MAX_SCOPE_BYTES: usize = 8 * 1024;
pub const MAX_CHUNK_BYTES: usize = 256 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagingOwner {
    connection_id: [u8; 16],
    principal_id: [u8; 16],
}

impl StagingOwner {
    /// Bind staging authority to a connection after gateway authentication.
    /// Never construct this value from client-supplied identity fields.
    pub fn authenticated(connection_id: [u8; 16], principal_id: [u8; 16]) -> Self {
        Self {
            connection_id,
            principal_id,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StagingLimits {
    pub max_global_reserved_bytes: u64,
    pub max_principal_reserved_bytes: u64,
    pub max_artifact_bytes: u64,
    pub max_parameter_bytes: u64,
    pub max_live_reservations: usize,
    pub reservation_ttl: Duration,
}

impl Default for StagingLimits {
    fn default() -> Self {
        Self {
            max_global_reserved_bytes: 256 * 1024 * 1024,
            max_principal_reserved_bytes: MAX_ARTIFACT_BYTES
                + MAX_PARAMETER_BYTES
                + MAX_MANIFEST_BYTES as u64
                + MAX_SCOPE_BYTES as u64,
            max_artifact_bytes: MAX_ARTIFACT_BYTES,
            max_parameter_bytes: MAX_PARAMETER_BYTES,
            max_live_reservations: 1024,
            reservation_ttl: Duration::from_secs(300),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PrepareSubmission {
    pub manifest_version: u32,
    pub manifest: Vec<u8>,
    pub artifact_bytes: u64,
    pub artifact_sha256: [u8; 32],
    pub scope_version: u32,
    pub requested_scope: Vec<u8>,
    pub parameter_bytes: u64,
    pub parameter_sha256: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmissionMetadata {
    pub manifest_version: u32,
    pub manifest: Vec<u8>,
    pub scope_version: u32,
    pub requested_scope: Vec<u8>,
}

/// Trusted supervisor logic validates versioned metadata without loading the
/// artifact. Unsupported decoders must return false from `supports_versions`.
pub trait SubmissionMetadataValidator: Send + Sync {
    fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool;
    fn validate(&self, metadata: &SubmissionMetadata) -> bool;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChunkKind {
    Artifact,
    Parameters,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagingReservation {
    pub invocation_id: [u8; 16],
    pub artifact_next_offset: u64,
    pub parameter_next_offset: u64,
}

#[derive(Debug)]
pub struct SealedSubmission {
    invocation_id: [u8; 16],
    data: Arc<SealedSubmissionData>,
}

#[derive(Debug)]
struct SealedSubmissionData {
    metadata: SubmissionMetadata,
    artifact: Arc<[u8]>,
    parameters: Arc<[u8]>,
}

impl SealedSubmission {
    pub fn invocation_id(&self) -> [u8; 16] {
        self.invocation_id
    }

    pub fn metadata(&self) -> &SubmissionMetadata {
        &self.data.metadata
    }

    pub fn artifact(&self) -> &[u8] {
        &self.data.artifact
    }

    pub fn parameters(&self) -> &[u8] {
        &self.data.parameters
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReservationState {
    Uploading,
    Sealing,
    DiscardRequested,
    Sealed,
    Starting,
    Started,
}

struct Reservation {
    connection_id: [u8; 16],
    principal_id: [u8; 16],
    metadata: SubmissionMetadata,
    artifact_bytes: u64,
    artifact_sha256: [u8; 32],
    parameter_bytes: u64,
    parameter_sha256: [u8; 32],
    artifact: Vec<u8>,
    parameters: Vec<u8>,
    sealed: Option<Arc<SealedSubmissionData>>,
    state: ReservationState,
    created_at: Instant,
}

#[derive(Default)]
struct StagingState {
    reservations: HashMap<[u8; 16], Reservation>,
    connection_reservations: HashMap<[u8; 16], [u8; 16]>,
    global_reserved_bytes: u64,
    principal_reserved_bytes: HashMap<[u8; 16], u64>,
}

pub struct StagingStore {
    limits: StagingLimits,
    metadata_validator: Arc<dyn SubmissionMetadataValidator>,
    state: Mutex<StagingState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StagingError {
    InvalidLimits,
    InvalidRequest,
    UnsupportedMetadataVersion,
    ResourceLimit,
    AllocationFailed,
    RandomnessUnavailable,
    Poisoned,
    Unavailable,
    InvalidState,
    InvalidChunk,
    InvalidContentLength,
    DigestMismatch,
    InvalidMetadata,
}

impl std::fmt::Display for StagingError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "submission staging failed: {self:?}")
    }
}

impl std::error::Error for StagingError {}

#[derive(Debug)]
pub enum StartError<E> {
    Staging(StagingError),
    Admission(E),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConnectionClose {
    Discarded(Vec<[u8; 16]>),
    CancelStarted(Vec<[u8; 16]>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmissionState {
    Uploading,
    Sealing,
    Prepared,
    Starting,
    Started,
}

impl StagingStore {
    pub fn new(
        limits: StagingLimits,
        metadata_validator: Arc<dyn SubmissionMetadataValidator>,
    ) -> Result<Self, StagingError> {
        if limits.max_global_reserved_bytes == 0
            || limits.max_principal_reserved_bytes == 0
            || limits.max_artifact_bytes == 0
            || limits.max_artifact_bytes > MAX_ARTIFACT_BYTES
            || limits.max_parameter_bytes > MAX_PARAMETER_BYTES
            || limits.max_live_reservations == 0
            || limits.reservation_ttl.is_zero()
        {
            return Err(StagingError::InvalidLimits);
        }
        Ok(Self {
            limits,
            metadata_validator,
            state: Mutex::new(StagingState::default()),
        })
    }

    /// Reserve one invocation ID for this authenticated connection. This only
    /// reserves bounded staging capacity; it creates no storage transaction.
    pub fn prepare(
        &self,
        owner: StagingOwner,
        request: PrepareSubmission,
    ) -> Result<StagingReservation, StagingError> {
        if !self
            .metadata_validator
            .supports_versions(request.manifest_version, request.scope_version)
        {
            return Err(StagingError::UnsupportedMetadataVersion);
        }
        if owner.connection_id == [0; 16]
            || owner.principal_id == [0; 16]
            || request.artifact_bytes == 0
            || request.artifact_bytes > self.limits.max_artifact_bytes
            || request.parameter_bytes > self.limits.max_parameter_bytes
            || request.manifest.is_empty()
            || request.manifest.len() > MAX_MANIFEST_BYTES
            || request.requested_scope.is_empty()
            || request.requested_scope.len() > MAX_SCOPE_BYTES
        {
            return Err(StagingError::InvalidRequest);
        }
        let reservation_size = request
            .artifact_bytes
            .checked_add(request.parameter_bytes)
            .and_then(|size| size.checked_add(request.manifest.len() as u64))
            .and_then(|size| size.checked_add(request.requested_scope.len() as u64))
            .ok_or(StagingError::InvalidRequest)?;
        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        reap_expired(&mut state, self.limits.reservation_ttl, Instant::now());
        if state
            .connection_reservations
            .contains_key(&owner.connection_id)
        {
            return Err(StagingError::InvalidState);
        }
        if state.reservations.len() >= self.limits.max_live_reservations {
            return Err(StagingError::ResourceLimit);
        }
        let next_global = state
            .global_reserved_bytes
            .checked_add(reservation_size)
            .ok_or(StagingError::ResourceLimit)?;
        let principal_bytes = state
            .principal_reserved_bytes
            .get(&owner.principal_id)
            .copied()
            .unwrap_or_default();
        let next_principal = principal_bytes
            .checked_add(reservation_size)
            .ok_or(StagingError::ResourceLimit)?;
        if next_global > self.limits.max_global_reserved_bytes
            || next_principal > self.limits.max_principal_reserved_bytes
        {
            return Err(StagingError::ResourceLimit);
        }

        let invocation_id = generate_invocation_id(&state)?;
        state.reservations.insert(
            invocation_id,
            Reservation {
                connection_id: owner.connection_id,
                principal_id: owner.principal_id,
                metadata: SubmissionMetadata {
                    manifest_version: request.manifest_version,
                    manifest: request.manifest,
                    scope_version: request.scope_version,
                    requested_scope: request.requested_scope,
                },
                artifact_bytes: request.artifact_bytes,
                artifact_sha256: request.artifact_sha256,
                parameter_bytes: request.parameter_bytes,
                parameter_sha256: request.parameter_sha256,
                artifact: Vec::new(),
                parameters: Vec::new(),
                sealed: None,
                state: ReservationState::Uploading,
                created_at: Instant::now(),
            },
        );
        state
            .connection_reservations
            .insert(owner.connection_id, invocation_id);
        state.global_reserved_bytes = next_global;
        state
            .principal_reserved_bytes
            .insert(owner.principal_id, next_principal);
        Ok(StagingReservation {
            invocation_id,
            artifact_next_offset: 0,
            parameter_next_offset: 0,
        })
    }

    /// Append one contiguous artifact or parameter chunk.
    pub fn upload_chunk(
        &self,
        owner: StagingOwner,
        invocation_id: [u8; 16],
        kind: ChunkKind,
        offset: u64,
        data: &[u8],
    ) -> Result<u64, StagingError> {
        if data.is_empty() || data.len() > MAX_CHUNK_BYTES {
            return Err(StagingError::InvalidChunk);
        }
        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        reap_expired(&mut state, self.limits.reservation_ttl, Instant::now());
        let reservation = owned_reservation_mut(&mut state, owner, invocation_id)?;
        if reservation.state != ReservationState::Uploading {
            return Err(StagingError::InvalidState);
        }
        let (buffer, declared_bytes) = match kind {
            ChunkKind::Artifact => (&mut reservation.artifact, reservation.artifact_bytes),
            ChunkKind::Parameters => (&mut reservation.parameters, reservation.parameter_bytes),
        };
        let expected_offset =
            u64::try_from(buffer.len()).map_err(|_| StagingError::InvalidChunk)?;
        if offset != expected_offset {
            return Err(StagingError::InvalidChunk);
        }
        let next_offset = expected_offset
            .checked_add(u64::try_from(data.len()).map_err(|_| StagingError::InvalidChunk)?)
            .ok_or(StagingError::InvalidChunk)?;
        if next_offset > declared_bytes {
            return Err(StagingError::InvalidContentLength);
        }
        buffer
            .try_reserve(data.len())
            .map_err(|_| StagingError::AllocationFailed)?;
        buffer.extend_from_slice(data);
        Ok(next_offset)
    }

    /// Verify declared lengths and hashes, validate versioned metadata, and
    /// freeze the upload into immutable shared buffers.
    pub fn seal(&self, owner: StagingOwner, invocation_id: [u8; 16]) -> Result<(), StagingError> {
        let (sealed_data, artifact_sha256, parameter_sha256) = {
            let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
            reap_expired(&mut state, self.limits.reservation_ttl, Instant::now());
            let reservation = owned_reservation_mut(&mut state, owner, invocation_id)?;
            if reservation.state != ReservationState::Uploading {
                return Err(StagingError::InvalidState);
            }
            if reservation.artifact.len() as u64 != reservation.artifact_bytes
                || reservation.parameters.len() as u64 != reservation.parameter_bytes
            {
                discard_reservation(&mut state, invocation_id);
                return Err(StagingError::InvalidContentLength);
            }
            let artifact_sha256 = reservation.artifact_sha256;
            let parameter_sha256 = reservation.parameter_sha256;
            let sealed_data = Arc::new(SealedSubmissionData {
                metadata: reservation.metadata.clone(),
                artifact: Arc::from(std::mem::take(&mut reservation.artifact)),
                parameters: Arc::from(std::mem::take(&mut reservation.parameters)),
            });
            reservation.sealed = Some(Arc::clone(&sealed_data));
            reservation.state = ReservationState::Sealing;
            (sealed_data, artifact_sha256, parameter_sha256)
        };

        let digests_match = Sha256::digest(&sealed_data.artifact).as_slice() == artifact_sha256
            && Sha256::digest(&sealed_data.parameters).as_slice() == parameter_sha256;
        if !digests_match {
            let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
            discard_reservation(&mut state, invocation_id);
            return Err(StagingError::DigestMismatch);
        }
        let metadata_is_valid = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.metadata_validator.validate(&sealed_data.metadata)
        }))
        .unwrap_or(false);
        if !metadata_is_valid {
            let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
            discard_reservation(&mut state, invocation_id);
            return Err(StagingError::InvalidMetadata);
        }

        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        let reservation = state
            .reservations
            .get_mut(&invocation_id)
            .filter(|reservation| {
                reservation.connection_id == owner.connection_id
                    && reservation.principal_id == owner.principal_id
            })
            .ok_or(StagingError::Unavailable)?;
        if reservation.state == ReservationState::DiscardRequested {
            discard_reservation(&mut state, invocation_id);
            return Err(StagingError::Unavailable);
        }
        if reservation.state != ReservationState::Sealing {
            return Err(StagingError::InvalidState);
        }
        reservation.state = ReservationState::Sealed;
        Ok(())
    }

    /// Revalidate current admission exactly once before the caller creates a
    /// storage snapshot or launches a worker. The callback must clean up any
    /// partial admission before returning an error. Failed starts cannot replay.
    pub fn start_with<T, E>(
        &self,
        owner: StagingOwner,
        invocation_id: [u8; 16],
        revalidate_and_start: impl FnOnce(&SealedSubmission) -> Result<T, E>,
    ) -> Result<T, StartError<E>> {
        let submission = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| StartError::Staging(StagingError::Poisoned))?;
            reap_expired(&mut state, self.limits.reservation_ttl, Instant::now());
            let reservation = owned_reservation_mut(&mut state, owner, invocation_id)
                .map_err(StartError::Staging)?;
            if reservation.state != ReservationState::Sealed {
                return Err(StartError::Staging(StagingError::InvalidState));
            }
            reservation.state = ReservationState::Starting;
            SealedSubmission {
                invocation_id,
                data: Arc::clone(
                    reservation
                        .sealed
                        .as_ref()
                        .ok_or(StartError::Staging(StagingError::InvalidState))?,
                ),
            }
        };

        let result = revalidate_and_start(&submission);
        let mut state = self
            .state
            .lock()
            .map_err(|_| StartError::Staging(StagingError::Poisoned))?;
        let is_starting = state
            .reservations
            .get(&invocation_id)
            .filter(|reservation| {
                reservation.connection_id == owner.connection_id
                    && reservation.principal_id == owner.principal_id
            })
            .is_some_and(|reservation| reservation.state == ReservationState::Starting);
        if !is_starting {
            return Err(StartError::Staging(StagingError::InvalidState));
        }
        match result {
            Ok(value) => {
                state
                    .reservations
                    .get_mut(&invocation_id)
                    .ok_or(StartError::Staging(StagingError::Unavailable))?
                    .state = ReservationState::Started;
                Ok(value)
            }
            Err(error) => {
                discard_reservation(&mut state, invocation_id);
                Err(StartError::Admission(error))
            }
        }
    }

    /// Release sealed bytes and quota after the invocation reaches a durable
    /// terminal outcome.
    pub fn finish_started(
        &self,
        owner: StagingOwner,
        invocation_id: [u8; 16],
    ) -> Result<(), StagingError> {
        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        let reservation = state
            .reservations
            .get(&invocation_id)
            .filter(|reservation| {
                reservation.connection_id == owner.connection_id
                    && reservation.principal_id == owner.principal_id
            })
            .ok_or(StagingError::Unavailable)?;
        if reservation.state != ReservationState::Started {
            return Err(StagingError::InvalidState);
        }
        discard_reservation(&mut state, invocation_id);
        Ok(())
    }

    /// Cancel an owner-scoped reservation without starting a storage
    /// transaction. A concurrent seal is marked for discard when validation
    /// finishes; started work must use invocation cancellation instead.
    pub fn cancel_prepared(
        &self,
        owner: StagingOwner,
        invocation_id: [u8; 16],
    ) -> Result<(), StagingError> {
        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        reap_expired(&mut state, self.limits.reservation_ttl, Instant::now());
        let reservation = owned_reservation_mut(&mut state, owner, invocation_id)?;
        match reservation.state {
            ReservationState::Uploading | ReservationState::Sealed => {
                discard_reservation(&mut state, invocation_id);
                Ok(())
            }
            ReservationState::Sealing => {
                reservation.state = ReservationState::DiscardRequested;
                Ok(())
            }
            ReservationState::DiscardRequested => Ok(()),
            ReservationState::Starting | ReservationState::Started => {
                Err(StagingError::InvalidState)
            }
        }
    }

    /// Return an owner-scoped ephemeral staging state; unknown and foreign IDs
    /// share one unavailable result.
    pub fn status(
        &self,
        owner: StagingOwner,
        invocation_id: [u8; 16],
    ) -> Result<SubmissionState, StagingError> {
        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        reap_expired(&mut state, self.limits.reservation_ttl, Instant::now());
        let reservation = owned_reservation_mut(&mut state, owner, invocation_id)?;
        match reservation.state {
            ReservationState::Uploading => Ok(SubmissionState::Uploading),
            ReservationState::Sealing => Ok(SubmissionState::Sealing),
            ReservationState::Sealed => Ok(SubmissionState::Prepared),
            ReservationState::Starting => Ok(SubmissionState::Starting),
            ReservationState::Started => Ok(SubmissionState::Started),
            ReservationState::DiscardRequested => Err(StagingError::Unavailable),
        }
    }

    /// Reap expired reservations that have not started and return their IDs.
    /// A trusted supervisor should call this periodically as well as relying
    /// on the opportunistic reaping performed by mutating operations.
    pub fn reap_expired(&self) -> Result<Vec<[u8; 16]>, StagingError> {
        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        Ok(reap_expired(
            &mut state,
            self.limits.reservation_ttl,
            Instant::now(),
        ))
    }

    /// Discard pre-start uploads on disconnect and return running IDs for
    /// cancellation by the supervisor.
    pub fn close_connection(&self, owner: StagingOwner) -> Result<ConnectionClose, StagingError> {
        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        let Some(invocation_id) = state
            .connection_reservations
            .get(&owner.connection_id)
            .copied()
        else {
            return Ok(ConnectionClose::Discarded(Vec::new()));
        };
        let reservation = state
            .reservations
            .get(&invocation_id)
            .filter(|reservation| reservation.principal_id == owner.principal_id)
            .ok_or(StagingError::Unavailable)?;
        match reservation.state {
            ReservationState::Starting | ReservationState::Started => {
                Ok(ConnectionClose::CancelStarted(vec![invocation_id]))
            }
            ReservationState::Sealing => {
                state
                    .reservations
                    .get_mut(&invocation_id)
                    .ok_or(StagingError::Unavailable)?
                    .state = ReservationState::DiscardRequested;
                Ok(ConnectionClose::Discarded(vec![invocation_id]))
            }
            ReservationState::DiscardRequested => {
                Ok(ConnectionClose::Discarded(vec![invocation_id]))
            }
            ReservationState::Uploading | ReservationState::Sealed => {
                discard_reservation(&mut state, invocation_id);
                Ok(ConnectionClose::Discarded(vec![invocation_id]))
            }
        }
    }
}

fn owned_reservation_mut(
    state: &mut StagingState,
    owner: StagingOwner,
    invocation_id: [u8; 16],
) -> Result<&mut Reservation, StagingError> {
    state
        .reservations
        .get_mut(&invocation_id)
        .filter(|reservation| {
            reservation.connection_id == owner.connection_id
                && reservation.principal_id == owner.principal_id
        })
        .ok_or(StagingError::Unavailable)
}

fn discard_reservation(state: &mut StagingState, invocation_id: [u8; 16]) {
    let Some(reservation) = state.reservations.remove(&invocation_id) else {
        return;
    };
    state
        .connection_reservations
        .remove(&reservation.connection_id);
    let reservation_size = reservation
        .artifact_bytes
        .saturating_add(reservation.parameter_bytes)
        .saturating_add(reservation.metadata.manifest.len() as u64)
        .saturating_add(reservation.metadata.requested_scope.len() as u64);
    state.global_reserved_bytes = state.global_reserved_bytes.saturating_sub(reservation_size);
    if let Some(principal_bytes) = state
        .principal_reserved_bytes
        .get_mut(&reservation.principal_id)
    {
        *principal_bytes = principal_bytes.saturating_sub(reservation_size);
        if *principal_bytes == 0 {
            state
                .principal_reserved_bytes
                .remove(&reservation.principal_id);
        }
    }
}

fn reap_expired(state: &mut StagingState, ttl: Duration, now: Instant) -> Vec<[u8; 16]> {
    let expired = state
        .reservations
        .iter()
        .filter_map(|(invocation_id, reservation)| {
            (matches!(
                reservation.state,
                ReservationState::Uploading | ReservationState::Sealed
            ) && now.saturating_duration_since(reservation.created_at) >= ttl)
                .then_some(*invocation_id)
        })
        .collect::<Vec<_>>();
    for invocation_id in &expired {
        discard_reservation(state, *invocation_id);
    }
    expired
}

fn generate_invocation_id(state: &StagingState) -> Result<[u8; 16], StagingError> {
    for _ in 0..8 {
        let mut invocation_id = [0u8; 16];
        getrandom::fill(&mut invocation_id).map_err(|_| StagingError::RandomnessUnavailable)?;
        if invocation_id != [0; 16] && !state.reservations.contains_key(&invocation_id) {
            return Ok(invocation_id);
        }
    }
    Err(StagingError::RandomnessUnavailable)
}
