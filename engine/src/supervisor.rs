//! Bounded in-memory submission staging for the trusted supervisor.
//!
//! Staging does not open a storage snapshot or load native code. The caller
//! must bind `StagingOwner` to an already authenticated query connection.

use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Unbounded};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const MAX_ARTIFACT_BYTES: u64 = 64 * 1024 * 1024;
pub const MAX_PARAMETER_BYTES: u64 = 1024 * 1024;
pub const MAX_MANIFEST_BYTES: usize = 16 * 1024;
pub const MAX_SCOPE_BYTES: usize = 8 * 1024;
pub const MAX_CHUNK_BYTES: usize = 256 * 1024;
pub const DEFAULT_MAX_DEADLINE_MS: u64 = 15 * 60 * 1000;
const OPPORTUNISTIC_REAPER_SCAN_LIMIT: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagingOwner {
    connection_id: [u8; 16],
    principal_id: [u8; 16],
}

impl StagingOwner {
    /// Bind staging authority to a connection after gateway authentication.
    /// Never construct this value from client-supplied identity fields.
    pub(crate) fn authenticated(connection_id: [u8; 16], principal_id: [u8; 16]) -> Self {
        Self {
            connection_id,
            principal_id,
        }
    }

    pub(crate) fn principal_id(self) -> [u8; 16] {
        self.principal_id
    }
}

#[derive(Clone, Debug)]
pub struct StagingLimits {
    pub max_global_reserved_bytes: u64,
    pub max_principal_reserved_bytes: u64,
    pub max_artifact_bytes: u64,
    pub max_parameter_bytes: u64,
    pub max_deadline_ms: u64,
    pub max_live_reservations: usize,
    /// Maximum reservations entering or executing START with operational limits.
    pub max_active_operational: usize,
    /// Maximum reservations entering or executing START with analytical limits.
    pub max_active_analytical: usize,
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
            max_deadline_ms: DEFAULT_MAX_DEADLINE_MS,
            max_live_reservations: 1024,
            max_active_operational: 64,
            max_active_analytical: 8,
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
    pub deadline_ms: u64,
    pub workload_class: WorkloadClass,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkloadClass {
    Operational,
    Analytical,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SubmissionMetadata {
    pub manifest_version: u32,
    pub manifest: Vec<u8>,
    pub scope_version: u32,
    pub requested_scope: Vec<u8>,
    pub deadline_ms: u64,
    pub workload_class: WorkloadClass,
}

/// Trusted supervisor logic validates versioned metadata without loading the
/// artifact. Unsupported decoders must return false from `supports_versions`.
pub trait SubmissionMetadataValidator: Send + Sync {
    fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool;
    fn validate(&self, metadata: &SubmissionMetadata) -> bool;

    /// Validate metadata against the pinned artifact bytes without loading or
    /// executing them. Simple test validators may keep the metadata-only rule.
    fn validate_artifact(&self, metadata: &SubmissionMetadata, _artifact: &[u8]) -> bool {
        self.validate(metadata)
    }
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
    reservations: BTreeMap<[u8; 16], Reservation>,
    connection_reservations: HashMap<[u8; 16], [u8; 16]>,
    global_reserved_bytes: u64,
    principal_reserved_bytes: HashMap<[u8; 16], u64>,
    reaper_cursor: Option<[u8; 16]>,
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
    Sandbox(crate::linux_sandbox::SandboxCapabilityError),
    WorkerSetup(std::io::Error),
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
            || limits.max_deadline_ms == 0
            || limits.max_live_reservations == 0
            || limits.max_active_operational == 0
            || limits.max_active_analytical == 0
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
            || request.deadline_ms == 0
            || request.deadline_ms > self.limits.max_deadline_ms
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
        reap_expired(
            &mut state,
            self.limits.reservation_ttl,
            Instant::now(),
            OPPORTUNISTIC_REAPER_SCAN_LIMIT,
        );
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
                    deadline_ms: request.deadline_ms,
                    workload_class: request.workload_class,
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
        reap_expired(
            &mut state,
            self.limits.reservation_ttl,
            Instant::now(),
            OPPORTUNISTIC_REAPER_SCAN_LIMIT,
        );
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
            reap_expired(
                &mut state,
                self.limits.reservation_ttl,
                Instant::now(),
                OPPORTUNISTIC_REAPER_SCAN_LIMIT,
            );
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
        if sealed_data.metadata.scope_version == 1
            && crate::submission_scope::decode_requested_scope_v1(
                &sealed_data.metadata.requested_scope,
            )
            .is_err()
        {
            let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
            discard_reservation(&mut state, invocation_id);
            return Err(StagingError::InvalidMetadata);
        }
        let metadata_is_valid = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.metadata_validator
                .validate_artifact(&sealed_data.metadata, &sealed_data.artifact)
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
            reap_expired(
                &mut state,
                self.limits.reservation_ttl,
                Instant::now(),
                OPPORTUNISTIC_REAPER_SCAN_LIMIT,
            );
            let reservation = state
                .reservations
                .get(&invocation_id)
                .filter(|reservation| {
                    reservation.connection_id == owner.connection_id
                        && reservation.principal_id == owner.principal_id
                })
                .ok_or(StartError::Staging(StagingError::Unavailable))?;
            if reservation.state != ReservationState::Sealed {
                return Err(StartError::Staging(StagingError::InvalidState));
            }
            let workload_class = reservation.metadata.workload_class;
            let active_count = state
                .reservations
                .values()
                .filter(|reservation| {
                    matches!(
                        reservation.state,
                        ReservationState::Starting | ReservationState::Started
                    ) && reservation.metadata.workload_class == workload_class
                })
                .count();
            let active_limit = match workload_class {
                WorkloadClass::Operational => self.limits.max_active_operational,
                WorkloadClass::Analytical => self.limits.max_active_analytical,
            };
            if active_count >= active_limit {
                return Err(StartError::Staging(StagingError::ResourceLimit));
            }
            let submission = SealedSubmission {
                invocation_id,
                data: Arc::clone(
                    reservation
                        .sealed
                        .as_ref()
                        .ok_or(StartError::Staging(StagingError::InvalidState))?,
                ),
            };
            state
                .reservations
                .get_mut(&invocation_id)
                .ok_or(StartError::Staging(StagingError::Unavailable))?
                .state = ReservationState::Starting;
            submission
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

    /// Check the configured host profile before entering storage admission.
    /// The callback must still establish confinement and provision worker
    /// resources before opening or executing the sealed artifact.
    pub fn start_with_sandbox_preflight<T, E>(
        &self,
        owner: StagingOwner,
        invocation_id: [u8; 16],
        cgroup_parent: impl AsRef<std::path::Path>,
        revalidate_and_start: impl FnOnce(&SealedSubmission) -> Result<T, E>,
    ) -> Result<T, StartError<E>> {
        crate::linux_sandbox::probe(cgroup_parent)
            .require_supported()
            .map_err(StartError::Sandbox)?;
        self.start_with(owner, invocation_id, revalidate_and_start)
    }

    /// Create the invocation cgroup before consuming the prepared reservation
    /// or opening storage admission. The callback owns the cgroup and must
    /// place the executor in it before untrusted code can run, and clean up
    /// the cgroup if launch fails. If setup fails, the reservation remains
    /// prepared for retry.
    pub fn start_with_worker_cgroup<T, E>(
        &self,
        owner: StagingOwner,
        invocation_id: [u8; 16],
        cgroup_parent: impl AsRef<std::path::Path>,
        limits: crate::linux_sandbox::WorkerResourceLimits,
        revalidate_and_launch: impl FnOnce(
            &SealedSubmission,
            crate::linux_sandbox::WorkerCgroup,
        ) -> Result<T, E>,
    ) -> Result<T, StartError<E>> {
        self.start_with_worker_cgroup_selected(
            owner,
            invocation_id,
            cgroup_parent,
            |_| limits,
            revalidate_and_launch,
        )
    }

    /// Select resource limits from immutable sealed metadata before creating
    /// the invocation cgroup. The sealed reservation is revalidated by
    /// `start_with` after setup, so cancellation and one-shot START semantics
    /// remain authoritative.
    pub fn start_with_worker_cgroup_selected<T, E>(
        &self,
        owner: StagingOwner,
        invocation_id: [u8; 16],
        cgroup_parent: impl AsRef<std::path::Path>,
        select_limits: impl FnOnce(&SubmissionMetadata) -> crate::linux_sandbox::WorkerResourceLimits,
        revalidate_and_launch: impl FnOnce(
            &SealedSubmission,
            crate::linux_sandbox::WorkerCgroup,
        ) -> Result<T, E>,
    ) -> Result<T, StartError<E>> {
        let cgroup_parent = cgroup_parent.as_ref();
        crate::linux_sandbox::probe(cgroup_parent)
            .require_supported()
            .map_err(StartError::Sandbox)?;

        // Check readiness without retaining artifact bytes outside the
        // staging quota. Cancellation may still win; `start_with` rechecks
        // ownership and state before invoking the callback.
        let metadata = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| StartError::Staging(StagingError::Poisoned))?;
            reap_expired(
                &mut state,
                self.limits.reservation_ttl,
                Instant::now(),
                OPPORTUNISTIC_REAPER_SCAN_LIMIT,
            );
            let reservation = owned_reservation_mut(&mut state, owner, invocation_id)
                .map_err(StartError::Staging)?;
            if reservation.state != ReservationState::Sealed {
                return Err(StartError::Staging(StagingError::InvalidState));
            }
            let sealed = reservation
                .sealed
                .as_ref()
                .ok_or(StartError::Staging(StagingError::InvalidState))?;
            sealed.metadata.clone()
        };
        let limits = select_limits(&metadata);
        let cgroup =
            crate::linux_sandbox::WorkerCgroup::create(cgroup_parent, invocation_id, limits)
                .map_err(StartError::WorkerSetup)?;

        self.start_with(owner, invocation_id, |current_submission| {
            revalidate_and_launch(current_submission, cgroup)
        })
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
        reap_expired(
            &mut state,
            self.limits.reservation_ttl,
            Instant::now(),
            OPPORTUNISTIC_REAPER_SCAN_LIMIT,
        );
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
        reap_expired(
            &mut state,
            self.limits.reservation_ttl,
            Instant::now(),
            OPPORTUNISTIC_REAPER_SCAN_LIMIT,
        );
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

    /// Return immutable submission bytes only after the owning reservation
    /// has crossed the one-shot START transition. This handle is for trusted
    /// supervisor launch code; it does not load or execute the artifact.
    pub fn started_submission(
        &self,
        owner: StagingOwner,
        invocation_id: [u8; 16],
    ) -> Result<SealedSubmission, StagingError> {
        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        reap_expired(
            &mut state,
            self.limits.reservation_ttl,
            Instant::now(),
            OPPORTUNISTIC_REAPER_SCAN_LIMIT,
        );
        let reservation = owned_reservation_mut(&mut state, owner, invocation_id)?;
        if reservation.state != ReservationState::Started {
            return Err(StagingError::InvalidState);
        }
        let data = reservation
            .sealed
            .as_ref()
            .cloned()
            .ok_or(StagingError::InvalidState)?;
        Ok(SealedSubmission {
            invocation_id,
            data,
        })
    }

    /// Reap at most `max_scanned` reservations that have not started and
    /// return the expired IDs. Repeated calls rotate through the ordered
    /// reservation set, so large configured stores are eventually covered
    /// without one maintenance pass scanning the whole store.
    pub fn reap_expired(&self, max_scanned: usize) -> Result<Vec<[u8; 16]>, StagingError> {
        if max_scanned == 0 {
            return Err(StagingError::InvalidRequest);
        }
        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        Ok(reap_expired(
            &mut state,
            self.limits.reservation_ttl,
            Instant::now(),
            max_scanned,
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

fn reap_expired(
    state: &mut StagingState,
    ttl: Duration,
    now: Instant,
    max_scanned: usize,
) -> Vec<[u8; 16]> {
    let mut scanned = Vec::with_capacity(max_scanned.min(state.reservations.len()));
    if let Some(cursor) = state.reaper_cursor {
        scanned.extend(
            state
                .reservations
                .range((Excluded(cursor), Unbounded))
                .take(max_scanned)
                .map(|(id, _)| *id),
        );
    }
    if scanned.len() < max_scanned {
        let remaining = max_scanned - scanned.len();
        scanned.extend(
            state
                .reservations
                .keys()
                .take(remaining)
                .filter(|id| state.reaper_cursor.is_none_or(|cursor| **id <= cursor))
                .copied(),
        );
    }
    if let Some(last_scanned) = scanned.last().copied() {
        state.reaper_cursor = Some(last_scanned);
    }
    let expired = scanned
        .iter()
        .filter_map(|invocation_id| {
            state
                .reservations
                .get(invocation_id)
                .and_then(|reservation| {
                    (matches!(
                        reservation.state,
                        ReservationState::Uploading | ReservationState::Sealed
                    ) && now.saturating_duration_since(reservation.created_at) >= ttl)
                        .then_some(*invocation_id)
                })
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    struct AcceptMetadata;

    impl SubmissionMetadataValidator for AcceptMetadata {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, metadata: &SubmissionMetadata) -> bool {
            metadata.manifest == b"manifest"
                && metadata.requested_scope == requested_scope_fixture()
        }
    }

    struct BlockingMetadata {
        entered: Barrier,
        resume: Barrier,
    }

    impl SubmissionMetadataValidator for BlockingMetadata {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, _metadata: &SubmissionMetadata) -> bool {
            self.entered.wait();
            self.resume.wait();
            true
        }
    }

    fn make_owner(connection: u8, principal: u8) -> StagingOwner {
        StagingOwner::authenticated([connection; 16], [principal; 16])
    }

    fn requested_scope_fixture() -> Vec<u8> {
        let mut bytes = b"PSC1\0\x01\x02\x02".to_vec();
        bytes.extend([1; 16]);
        bytes
    }

    fn request(artifact: &[u8], parameters: &[u8]) -> PrepareSubmission {
        request_for_class(artifact, parameters, WorkloadClass::Operational)
    }

    fn request_for_class(
        artifact: &[u8],
        parameters: &[u8],
        workload_class: WorkloadClass,
    ) -> PrepareSubmission {
        PrepareSubmission {
            manifest_version: 1,
            manifest: b"manifest".to_vec(),
            artifact_bytes: artifact.len() as u64,
            artifact_sha256: Sha256::digest(artifact).into(),
            scope_version: 1,
            requested_scope: requested_scope_fixture(),
            parameter_bytes: parameters.len() as u64,
            parameter_sha256: Sha256::digest(parameters).into(),
            deadline_ms: 30_000,
            workload_class,
        }
    }

    fn store(validator: Arc<dyn SubmissionMetadataValidator>) -> StagingStore {
        StagingStore::new(StagingLimits::default(), validator).unwrap()
    }

    fn upload_and_seal(
        store: &StagingStore,
        owner: StagingOwner,
        artifact: &[u8],
        parameters: &[u8],
    ) -> [u8; 16] {
        let reservation = store.prepare(owner, request(artifact, parameters)).unwrap();
        store
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                artifact,
            )
            .unwrap();
        if !parameters.is_empty() {
            store
                .upload_chunk(
                    owner,
                    reservation.invocation_id,
                    ChunkKind::Parameters,
                    0,
                    parameters,
                )
                .unwrap();
        }
        store.seal(owner, reservation.invocation_id).unwrap();
        reservation.invocation_id
    }

    #[test]
    fn sealed_submission_starts_once_and_releases_quota_at_finish() {
        let store = store(Arc::new(AcceptMetadata));
        let owner = make_owner(1, 2);
        let invocation_id = upload_and_seal(&store, owner, b"artifact", b"params");
        assert_eq!(
            store.status(owner, invocation_id).unwrap(),
            SubmissionState::Prepared
        );
        let started = store
            .start_with(owner, invocation_id, |submission| {
                assert_eq!(submission.artifact(), b"artifact");
                assert_eq!(submission.parameters(), b"params");
                assert_eq!(submission.metadata().deadline_ms, 30_000);
                assert_eq!(
                    submission.metadata().workload_class,
                    WorkloadClass::Operational
                );
                Ok::<_, ()>(submission.invocation_id())
            })
            .unwrap();
        assert_eq!(started, invocation_id);
        assert_eq!(
            store.status(owner, invocation_id).unwrap(),
            SubmissionState::Started
        );
        assert!(matches!(
            store.start_with(owner, invocation_id, |_| Ok::<_, ()>(())),
            Err(StartError::Staging(StagingError::InvalidState))
        ));
        store.finish_started(owner, invocation_id).unwrap();
        assert_eq!(
            store.status(owner, invocation_id),
            Err(StagingError::Unavailable)
        );
        let replacement = store.prepare(owner, request(b"x", b""));
        assert!(replacement.is_ok());
    }

    #[test]
    fn active_invocation_quotas_are_isolated_by_class_and_retain_prepared_work() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::mpsc;

        let store = StagingStore::new(
            StagingLimits {
                max_active_operational: 1,
                max_active_analytical: 1,
                ..StagingLimits::default()
            },
            Arc::new(AcceptMetadata),
        )
        .unwrap();
        let operational_owner = make_owner(31, 41);
        let queued_owner = make_owner(32, 42);
        let analytical_owner = make_owner(33, 43);
        let operational_id = upload_and_seal(&store, operational_owner, b"op1", b"");
        let queued_id = upload_and_seal(&store, queued_owner, b"op2", b"");
        let analytical_reservation = store
            .prepare(
                analytical_owner,
                request_for_class(b"an1", b"", WorkloadClass::Analytical),
            )
            .unwrap();
        store
            .upload_chunk(
                analytical_owner,
                analytical_reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                b"an1",
            )
            .unwrap();
        store
            .seal(analytical_owner, analytical_reservation.invocation_id)
            .unwrap();

        let (started_sender, started_receiver) = mpsc::sync_channel(0);
        let (resume_sender, resume_receiver) = mpsc::sync_channel(0);
        std::thread::scope(|scope| {
            let store_ref = &store;
            let starting = scope.spawn(move || {
                store_ref.start_with(operational_owner, operational_id, |_| {
                    started_sender.send(()).unwrap();
                    resume_receiver.recv().unwrap();
                    Ok::<_, ()>(())
                })
            });
            started_receiver.recv().unwrap();

            let callback_called = AtomicBool::new(false);
            assert!(matches!(
                store.start_with(queued_owner, queued_id, |_| {
                    callback_called.store(true, Ordering::SeqCst);
                    Ok::<_, ()>(())
                }),
                Err(StartError::Staging(StagingError::ResourceLimit))
            ));
            assert!(!callback_called.load(Ordering::SeqCst));
            assert_eq!(
                store.status(queued_owner, queued_id),
                Ok(SubmissionState::Prepared)
            );

            store
                .start_with(
                    analytical_owner,
                    analytical_reservation.invocation_id,
                    |_| Ok::<_, ()>(()),
                )
                .unwrap();
            store
                .finish_started(analytical_owner, analytical_reservation.invocation_id)
                .unwrap();

            resume_sender.send(()).unwrap();
            starting.join().unwrap().unwrap();
        });

        store
            .finish_started(operational_owner, operational_id)
            .unwrap();
        store
            .start_with(queued_owner, queued_id, |_| Ok::<_, ()>(()))
            .unwrap();
        store.finish_started(queued_owner, queued_id).unwrap();
    }

    #[test]
    fn sandbox_preflight_rejects_before_storage_admission() {
        let store = store(Arc::new(AcceptMetadata));
        let owner = make_owner(21, 22);
        let invocation_id = upload_and_seal(&store, owner, b"artifact", b"params");
        let cgroup_parent =
            std::env::temp_dir().join(format!("prdb-sandbox-preflight-{}", std::process::id()));
        std::fs::create_dir_all(&cgroup_parent).unwrap();
        let callback_called = std::sync::atomic::AtomicBool::new(false);

        let result =
            store.start_with_sandbox_preflight(owner, invocation_id, &cgroup_parent, |_| {
                callback_called.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, ()>(())
            });

        assert!(matches!(result, Err(StartError::Sandbox(_))));
        assert!(!callback_called.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            store.status(owner, invocation_id).unwrap(),
            SubmissionState::Prepared
        );
        std::fs::remove_dir_all(cgroup_parent).unwrap();
    }

    #[test]
    fn cgroup_setup_rejects_before_admission_and_preserves_prepared_submission() {
        let store = store(Arc::new(AcceptMetadata));
        let owner = make_owner(31, 32);
        let invocation_id = upload_and_seal(&store, owner, b"artifact", b"params");
        let cgroup_parent =
            std::env::temp_dir().join(format!("prdb-cgroup-setup-{}", std::process::id()));
        std::fs::create_dir_all(&cgroup_parent).unwrap();
        let callback_called = std::sync::atomic::AtomicBool::new(false);

        let result = store.start_with_worker_cgroup(
            owner,
            invocation_id,
            &cgroup_parent,
            crate::linux_sandbox::WorkerResourceLimits {
                cpu_quota_micros: 100_000,
                cpu_period_micros: 100_000,
                memory_max_bytes: 64 * 1024 * 1024,
                max_processes: 16,
            },
            |_, _| {
                callback_called.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, ()>(())
            },
        );

        assert!(matches!(
            result,
            Err(StartError::Sandbox(_) | StartError::WorkerSetup(_))
        ));
        assert!(!callback_called.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            store.status(owner, invocation_id).unwrap(),
            SubmissionState::Prepared
        );
        std::fs::remove_dir_all(cgroup_parent).unwrap();
    }

    #[test]
    fn executor_start_rejects_unsupported_host_before_storage_admission() {
        let store = store(Arc::new(AcceptMetadata));
        let owner = make_owner(33, 34);
        let invocation_id = upload_and_seal(&store, owner, b"artifact", b"params");
        let cgroup_parent =
            std::env::temp_dir().join(format!("prdb-executor-start-{}", std::process::id()));
        std::fs::create_dir_all(&cgroup_parent).unwrap();
        let admission_called = std::sync::atomic::AtomicBool::new(false);
        let config = crate::executor_launcher::ExecutorLaunchConfig {
            executor_path: std::path::PathBuf::from("/missing/prdb-executor"),
            scratch_parent: std::env::temp_dir(),
            runtime_read_execute: Vec::new(),
            runtime_read_only: Vec::new(),
            cleanup_timeout: std::time::Duration::from_millis(100),
        };

        let result = crate::executor_launcher::start_and_launch_executor(
            &store,
            owner,
            invocation_id,
            &cgroup_parent,
            crate::linux_sandbox::WorkerResourceLimits {
                cpu_quota_micros: 100_000,
                cpu_period_micros: 100_000,
                memory_max_bytes: 64 * 1024 * 1024,
                max_processes: 16,
            },
            &config,
            |_| {
                admission_called.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok::<_, ()>(())
            },
        );

        assert!(matches!(result, Err(StartError::Sandbox(_))));
        assert!(!admission_called.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(
            store.status(owner, invocation_id).unwrap(),
            SubmissionState::Prepared
        );
        std::fs::remove_dir_all(cgroup_parent).unwrap();
    }

    #[test]
    fn staging_rejects_cross_owner_and_noncontiguous_chunks() {
        let store = store(Arc::new(AcceptMetadata));
        let owner = make_owner(3, 4);
        let reservation = store.prepare(owner, request(b"artifact", b"")).unwrap();
        assert_eq!(
            store.upload_chunk(
                make_owner(5, 4),
                reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                b"artifact",
            ),
            Err(StagingError::Unavailable)
        );
        assert_eq!(
            store.upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                1,
                b"artifact",
            ),
            Err(StagingError::InvalidChunk)
        );
        assert_eq!(
            store.upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                b"artifact",
            ),
            Ok(8)
        );
        store.seal(owner, reservation.invocation_id).unwrap();
        assert_eq!(
            store.cancel_prepared(owner, reservation.invocation_id),
            Ok(())
        );
        assert_eq!(
            store.status(owner, reservation.invocation_id),
            Err(StagingError::Unavailable)
        );
    }

    #[test]
    fn cancellation_during_metadata_validation_discards_the_seal() {
        let validator = Arc::new(BlockingMetadata {
            entered: Barrier::new(2),
            resume: Barrier::new(2),
        });
        let store = Arc::new(store(validator.clone()));
        let owner = make_owner(6, 7);
        let reservation = store.prepare(owner, request(b"artifact", b"")).unwrap();
        store
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                b"artifact",
            )
            .unwrap();

        let sealer_store = Arc::clone(&store);
        let invocation_id = reservation.invocation_id;
        let sealer = std::thread::spawn(move || sealer_store.seal(owner, invocation_id));
        validator.entered.wait();
        store.cancel_prepared(owner, invocation_id).unwrap();
        validator.resume.wait();
        assert_eq!(sealer.join().unwrap(), Err(StagingError::Unavailable));
        assert_eq!(
            store.status(owner, invocation_id),
            Err(StagingError::Unavailable)
        );
    }

    #[test]
    fn disconnect_and_expiry_reclaim_unstarted_reservations() {
        let validator = Arc::new(AcceptMetadata);
        let store = store(validator.clone());
        let owner = make_owner(8, 9);
        let reservation = store.prepare(owner, request(b"artifact", b"")).unwrap();
        assert_eq!(
            store.close_connection(owner).unwrap(),
            ConnectionClose::Discarded(vec![reservation.invocation_id])
        );
        assert_eq!(
            store.status(owner, reservation.invocation_id),
            Err(StagingError::Unavailable)
        );

        let limits = StagingLimits {
            reservation_ttl: Duration::from_millis(1),
            ..StagingLimits::default()
        };
        let expiring_store = StagingStore::new(limits, validator).unwrap();
        let expiring_owner = make_owner(10, 9);
        let expiring = expiring_store
            .prepare(expiring_owner, request(b"artifact", b""))
            .unwrap();
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(
            expiring_store.reap_expired(usize::MAX).unwrap(),
            vec![expiring.invocation_id]
        );
        assert_eq!(
            expiring_store.status(expiring_owner, expiring.invocation_id),
            Err(StagingError::Unavailable)
        );
    }

    #[test]
    fn expiration_reaper_bounds_each_pass_and_rotates_across_reservations() {
        let limits = StagingLimits {
            reservation_ttl: Duration::from_millis(1),
            max_live_reservations: 4,
            ..StagingLimits::default()
        };
        let store = StagingStore::new(limits, Arc::new(AcceptMetadata)).unwrap();
        let reservations = (1..=3)
            .map(|connection| {
                let owner = make_owner(connection, 9);
                store
                    .prepare(owner, request(b"artifact", b""))
                    .map(|reservation| (owner, reservation))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        std::thread::sleep(Duration::from_millis(5));

        for _ in 0..reservations.len() {
            let reaped = store.reap_expired(1).unwrap();
            assert_eq!(reaped.len(), 1);
            assert!(
                reservations
                    .iter()
                    .any(|(_, reservation)| reservation.invocation_id == reaped[0])
            );
        }
        for (owner, reservation) in reservations {
            assert_eq!(
                store.status(owner, reservation.invocation_id),
                Err(StagingError::Unavailable)
            );
        }
        assert_eq!(store.reap_expired(0), Err(StagingError::InvalidRequest));
    }

    #[test]
    fn sealing_rejects_invalid_v1_scope_even_when_custom_validator_accepts_it() {
        struct AcceptEverything;
        impl SubmissionMetadataValidator for AcceptEverything {
            fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
                manifest_version == 1 && scope_version == 1
            }

            fn validate(&self, _metadata: &SubmissionMetadata) -> bool {
                true
            }
        }

        let store = store(Arc::new(AcceptEverything));
        let owner = make_owner(13, 14);
        let mut malformed_request = request(b"artifact", b"");
        malformed_request.requested_scope = b"scope".to_vec();
        let reservation = store.prepare(owner, malformed_request).unwrap();
        store
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                b"artifact",
            )
            .unwrap();
        assert_eq!(
            store.seal(owner, reservation.invocation_id),
            Err(StagingError::InvalidMetadata)
        );
        assert_eq!(
            store.status(owner, reservation.invocation_id),
            Err(StagingError::Unavailable)
        );
    }

    #[test]
    fn staging_preserves_workload_class_and_bounds_requested_deadline() {
        let limits = StagingLimits {
            max_deadline_ms: 10,
            ..StagingLimits::default()
        };
        let store = StagingStore::new(limits, Arc::new(AcceptMetadata)).unwrap();
        let owner = make_owner(11, 12);
        let mut too_long = request(b"artifact", b"");
        too_long.deadline_ms = 11;
        assert!(matches!(
            store.prepare(owner, too_long),
            Err(StagingError::InvalidRequest)
        ));
        let mut accepted = request(b"artifact", b"");
        accepted.deadline_ms = 10;
        accepted.workload_class = WorkloadClass::Analytical;
        let reservation = store.prepare(owner, accepted).unwrap();
        store
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                b"artifact",
            )
            .unwrap();
        store.seal(owner, reservation.invocation_id).unwrap();
        store
            .start_with(owner, reservation.invocation_id, |submission| {
                assert_eq!(submission.metadata().deadline_ms, 10);
                assert_eq!(
                    submission.metadata().workload_class,
                    WorkloadClass::Analytical
                );
                Ok::<_, ()>(())
            })
            .unwrap();
    }
}
