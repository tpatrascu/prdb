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
pub const MAX_SQL_DATA_BYTES: u64 = crate::query_abi::MAX_SQL_DATA_BYTES;
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
    pub max_sql_data_bytes: u64,
    pub max_deadline_ms: u64,
    pub max_live_reservations: usize,
    /// Maximum live reservations owned by one authenticated principal.
    pub max_live_reservations_per_principal: usize,
    /// Maximum non-active operational submissions uploading, sealing, or prepared.
    pub max_pending_operational: usize,
    /// Maximum non-active analytical submissions uploading, sealing, or prepared.
    pub max_pending_analytical: usize,
    /// Maximum reservations entering or executing START with operational limits.
    pub max_active_operational: usize,
    /// Maximum reservations entering or executing START with analytical limits.
    pub max_active_analytical: usize,
    /// Maximum active operational reservations owned by one principal.
    pub max_active_operational_per_principal: usize,
    /// Maximum active analytical reservations owned by one principal.
    pub max_active_analytical_per_principal: usize,
    pub reservation_ttl: Duration,
}

/// Best-effort, identifier-free operational view of supervisor staging and
/// START queue usage. The snapshot never includes principals or invocation IDs.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct StagingRuntimeUsage {
    pub pending_submissions: u64,
    pub starting_submissions: u64,
    pub started_submissions: u64,
    pub queued_operational_starts: u64,
    pub queued_analytical_starts: u64,
    pub oldest_queued_start_wait_us: u64,
    pub operational_start_wait_buckets: [u64; 6],
    pub analytical_start_wait_buckets: [u64; 6],
    pub reserved_submission_bytes: u64,
}

impl Default for StagingLimits {
    fn default() -> Self {
        Self {
            max_global_reserved_bytes: 256 * 1024 * 1024,
            max_principal_reserved_bytes: MAX_ARTIFACT_BYTES
                + MAX_PARAMETER_BYTES
                + MAX_SQL_DATA_BYTES
                + MAX_MANIFEST_BYTES as u64
                + MAX_SCOPE_BYTES as u64,
            max_artifact_bytes: MAX_ARTIFACT_BYTES,
            max_parameter_bytes: MAX_PARAMETER_BYTES,
            max_sql_data_bytes: MAX_SQL_DATA_BYTES,
            max_deadline_ms: DEFAULT_MAX_DEADLINE_MS,
            max_live_reservations: 1024,
            max_live_reservations_per_principal: 128,
            max_pending_operational: 256,
            max_pending_analytical: 64,
            max_active_operational: 64,
            max_active_analytical: 8,
            max_active_operational_per_principal: 16,
            max_active_analytical_per_principal: 2,
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
    pub sql_data_bytes: u64,
    pub sql_data_sha256: [u8; 32],
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

    /// Resolve shared SDK dependencies before storage admission. Returned pins
    /// remain owned by the started submission through worker completion.
    fn resolve_sdk_release(
        &self,
        _metadata: &SubmissionMetadata,
        _artifact: &[u8],
    ) -> Result<Option<crate::sdk_registry::SdkLibraryPin>, crate::submission_manifest::ManifestError>
    {
        Ok(None)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChunkKind {
    Artifact,
    Parameters,
    SqlData,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagingReservation {
    pub invocation_id: [u8; 16],
    pub artifact_next_offset: u64,
    pub parameter_next_offset: u64,
    pub sql_data_next_offset: u64,
}

#[derive(Debug)]
pub struct SealedSubmission {
    invocation_id: [u8; 16],
    data: Arc<SealedSubmissionData>,
    sdk_library_pin: Option<crate::sdk_registry::SdkLibraryPin>,
}

#[derive(Debug)]
struct SealedSubmissionData {
    metadata: SubmissionMetadata,
    interactive_package: Option<InteractivePackageBinding>,
    artifact: Arc<[u8]>,
    parameters: Arc<[u8]>,
    sql_data: Arc<[u8]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InteractivePackageBinding {
    pub transaction_id: [u8; 16],
    pub package_id: [u8; 16],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InteractivePackageIdentity {
    pub binding: InteractivePackageBinding,
    pub package_digest: [u8; 32],
    pub requested_scope: Vec<u8>,
}

impl SealedSubmission {
    pub fn invocation_id(&self) -> [u8; 16] {
        self.invocation_id
    }

    pub fn metadata(&self) -> &SubmissionMetadata {
        &self.data.metadata
    }

    /// Return server-retained transaction/package identity for protocol v2
    /// submissions. Artifact bytes cannot select or alter this binding.
    pub fn interactive_package(&self) -> Option<InteractivePackageBinding> {
        self.data.interactive_package
    }

    /// Hash the immutable package metadata and bytes using a domain-separated
    /// encoding for duplicate package ID detection in interactive SQL.
    pub fn interactive_package_digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"PRDB-INTERACTIVE-PACKAGE-1\0");
        digest.update(self.data.metadata.manifest_version.to_le_bytes());
        digest.update(self.data.metadata.scope_version.to_le_bytes());
        digest.update(self.data.metadata.deadline_ms.to_le_bytes());
        digest.update([match self.data.metadata.workload_class {
            WorkloadClass::Operational => 1,
            WorkloadClass::Analytical => 2,
        }]);
        for bytes in [
            self.data.metadata.manifest.as_slice(),
            self.data.metadata.requested_scope.as_slice(),
            self.data.artifact.as_ref(),
            self.data.parameters.as_ref(),
            self.data.sql_data.as_ref(),
        ] {
            digest.update((bytes.len() as u64).to_le_bytes());
            digest.update(bytes);
        }
        digest.finalize().into()
    }

    pub fn artifact(&self) -> &[u8] {
        &self.data.artifact
    }

    pub fn parameters(&self) -> &[u8] {
        &self.data.parameters
    }

    pub fn sql_data(&self) -> &[u8] {
        &self.data.sql_data
    }

    pub fn sdk_library_pin(&self) -> Option<&crate::sdk_registry::SdkLibraryPin> {
        self.sdk_library_pin.as_ref()
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
    interactive_package: Option<InteractivePackageBinding>,
    artifact_bytes: u64,
    artifact_sha256: [u8; 32],
    parameter_bytes: u64,
    parameter_sha256: [u8; 32],
    sql_data_bytes: u64,
    sql_data_sha256: [u8; 32],
    artifact: Vec<u8>,
    parameters: Vec<u8>,
    sql_data: Vec<u8>,
    sealed: Option<Arc<SealedSubmissionData>>,
    state: ReservationState,
    created_at: Instant,
}

#[derive(Default)]
struct StagingState {
    reservations: BTreeMap<[u8; 16], Reservation>,
    start_queue: BTreeMap<u128, StartQueueEntry>,
    operational_start_wait_buckets: [u64; 6],
    analytical_start_wait_buckets: [u64; 6],
    next_start_ticket: u128,
    connection_reservations: HashMap<[u8; 16], [u8; 16]>,
    global_reserved_bytes: u64,
    principal_reserved_bytes: HashMap<[u8; 16], u64>,
    reaper_cursor: Option<[u8; 16]>,
}

#[derive(Clone, Copy)]
struct StartQueueEntry {
    invocation_id: [u8; 16],
    principal_id: [u8; 16],
    workload_class: WorkloadClass,
    queued_at: Instant,
    was_blocked: bool,
}

pub struct StagingStore {
    limits: StagingLimits,
    metadata_validator: Arc<dyn SubmissionMetadataValidator>,
    sdk_repository: Option<Arc<crate::sdk_registry::SdkLibraryRepository>>,
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
            || limits.max_sql_data_bytes > MAX_SQL_DATA_BYTES
            || limits.max_deadline_ms == 0
            || limits.max_live_reservations == 0
            || limits.max_live_reservations_per_principal == 0
            || limits.max_pending_operational == 0
            || limits.max_pending_analytical == 0
            || limits.max_active_operational == 0
            || limits.max_active_analytical == 0
            || limits.max_active_operational_per_principal == 0
            || limits.max_active_analytical_per_principal == 0
            || limits.reservation_ttl.is_zero()
        {
            return Err(StagingError::InvalidLimits);
        }
        Ok(Self {
            limits,
            metadata_validator,
            sdk_repository: None,
            state: Mutex::new(StagingState::default()),
        })
    }

    /// Attach the instance-local SDK repository used by authenticated control
    /// operations. Query workers never receive this handle.
    pub fn with_sdk_repository(
        mut self,
        repository: Arc<crate::sdk_registry::SdkLibraryRepository>,
    ) -> Self {
        self.sdk_repository = Some(repository);
        self
    }

    pub fn sdk_repository(&self) -> Option<&Arc<crate::sdk_registry::SdkLibraryRepository>> {
        self.sdk_repository.as_ref()
    }

    /// Copy bounded aggregate queue usage for an authorized runtime response.
    /// No identifiers or per-principal data escape the staging lock.
    pub fn runtime_usage_summary(&self) -> Result<StagingRuntimeUsage, StagingError> {
        self.runtime_usage_summary_at(Instant::now())
    }

    fn runtime_usage_summary_at(
        &self,
        observed_at: Instant,
    ) -> Result<StagingRuntimeUsage, StagingError> {
        let state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        let mut summary = StagingRuntimeUsage {
            reserved_submission_bytes: state.global_reserved_bytes,
            operational_start_wait_buckets: state.operational_start_wait_buckets,
            analytical_start_wait_buckets: state.analytical_start_wait_buckets,
            ..StagingRuntimeUsage::default()
        };
        for reservation in state.reservations.values() {
            match reservation.state {
                ReservationState::Uploading
                | ReservationState::Sealing
                | ReservationState::DiscardRequested
                | ReservationState::Sealed => {
                    summary.pending_submissions = summary.pending_submissions.saturating_add(1);
                }
                ReservationState::Starting => {
                    summary.starting_submissions = summary.starting_submissions.saturating_add(1);
                }
                ReservationState::Started => {
                    summary.started_submissions = summary.started_submissions.saturating_add(1);
                }
            }
        }
        for queued in state.start_queue.values() {
            match queued.workload_class {
                WorkloadClass::Operational => {
                    summary.queued_operational_starts =
                        summary.queued_operational_starts.saturating_add(1);
                }
                WorkloadClass::Analytical => {
                    summary.queued_analytical_starts =
                        summary.queued_analytical_starts.saturating_add(1);
                }
            }
            summary.oldest_queued_start_wait_us = summary.oldest_queued_start_wait_us.max(
                u64::try_from(
                    observed_at
                        .saturating_duration_since(queued.queued_at)
                        .as_micros(),
                )
                .unwrap_or(u64::MAX),
            );
        }
        Ok(summary)
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
            || request.sql_data_bytes > self.limits.max_sql_data_bytes
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
            .and_then(|size| size.checked_add(request.sql_data_bytes))
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
        let principal_reservation_count = state
            .reservations
            .values()
            .filter(|reservation| reservation.principal_id == owner.principal_id)
            .count();
        if principal_reservation_count >= self.limits.max_live_reservations_per_principal {
            return Err(StagingError::ResourceLimit);
        }
        let pending_class_count = state
            .reservations
            .values()
            .filter(|reservation| {
                matches!(
                    reservation.state,
                    ReservationState::Uploading
                        | ReservationState::Sealing
                        | ReservationState::Sealed
                        | ReservationState::DiscardRequested
                ) && reservation.metadata.workload_class == request.workload_class
            })
            .count();
        let pending_class_limit = match request.workload_class {
            WorkloadClass::Operational => self.limits.max_pending_operational,
            WorkloadClass::Analytical => self.limits.max_pending_analytical,
        };
        if pending_class_count >= pending_class_limit {
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
                interactive_package: None,
                artifact_bytes: request.artifact_bytes,
                artifact_sha256: request.artifact_sha256,
                parameter_bytes: request.parameter_bytes,
                parameter_sha256: request.parameter_sha256,
                sql_data_bytes: request.sql_data_bytes,
                sql_data_sha256: request.sql_data_sha256,
                artifact: Vec::new(),
                parameters: Vec::new(),
                sql_data: Vec::new(),
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
            sql_data_next_offset: 0,
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
            ChunkKind::SqlData => (&mut reservation.sql_data, reservation.sql_data_bytes),
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

    /// Bind a protocol v2 staged package to a retained transaction. The
    /// binding is immutable after PREPARE and is copied into the sealed
    /// submission that trusted START orchestration receives.
    pub fn bind_interactive_package(
        &self,
        owner: StagingOwner,
        invocation_id: [u8; 16],
        transaction_id: [u8; 16],
        package_id: [u8; 16],
    ) -> Result<(), StagingError> {
        if transaction_id == [0; 16] || package_id == [0; 16] {
            return Err(StagingError::InvalidRequest);
        }
        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        reap_expired(
            &mut state,
            self.limits.reservation_ttl,
            Instant::now(),
            OPPORTUNISTIC_REAPER_SCAN_LIMIT,
        );
        let reservation = owned_reservation_mut(&mut state, owner, invocation_id)?;
        if reservation.state != ReservationState::Uploading
            || reservation.interactive_package.is_some()
        {
            return Err(StagingError::InvalidState);
        }
        reservation.interactive_package = Some(InteractivePackageBinding {
            transaction_id,
            package_id,
        });
        Ok(())
    }

    /// Read the immutable binding and digest of a sealed interactive package
    /// before START. The authenticated staging owner must match the PREPARE
    /// connection; neither opaque ID is accepted as authority.
    pub fn interactive_package_identity(
        &self,
        owner: StagingOwner,
        invocation_id: [u8; 16],
    ) -> Result<Option<InteractivePackageIdentity>, StagingError> {
        let mut state = self.state.lock().map_err(|_| StagingError::Poisoned)?;
        reap_expired(
            &mut state,
            self.limits.reservation_ttl,
            Instant::now(),
            OPPORTUNISTIC_REAPER_SCAN_LIMIT,
        );
        let reservation = owned_reservation_mut(&mut state, owner, invocation_id)?;
        if reservation.state != ReservationState::Sealed {
            return Err(StagingError::InvalidState);
        }
        let Some(binding) = reservation.interactive_package else {
            return Ok(None);
        };
        let data = reservation
            .sealed
            .as_ref()
            .ok_or(StagingError::InvalidState)?;
        let submission = SealedSubmission {
            invocation_id,
            data: Arc::clone(data),
            sdk_library_pin: None,
        };
        Ok(Some(InteractivePackageIdentity {
            binding,
            package_digest: submission.interactive_package_digest(),
            requested_scope: data.metadata.requested_scope.clone(),
        }))
    }

    /// Verify declared lengths and hashes, validate versioned metadata, and
    /// freeze the upload into immutable shared buffers.
    pub fn seal(&self, owner: StagingOwner, invocation_id: [u8; 16]) -> Result<(), StagingError> {
        let (sealed_data, artifact_sha256, parameter_sha256, sql_data_sha256) = {
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
                || reservation.sql_data.len() as u64 != reservation.sql_data_bytes
            {
                discard_reservation(&mut state, invocation_id);
                return Err(StagingError::InvalidContentLength);
            }
            let artifact_sha256 = reservation.artifact_sha256;
            let parameter_sha256 = reservation.parameter_sha256;
            let sql_data_sha256 = reservation.sql_data_sha256;
            let sealed_data = Arc::new(SealedSubmissionData {
                metadata: reservation.metadata.clone(),
                interactive_package: reservation.interactive_package,
                artifact: Arc::from(std::mem::take(&mut reservation.artifact)),
                parameters: Arc::from(std::mem::take(&mut reservation.parameters)),
                sql_data: Arc::from(std::mem::take(&mut reservation.sql_data)),
            });
            reservation.sealed = Some(Arc::clone(&sealed_data));
            reservation.state = ReservationState::Sealing;
            (
                sealed_data,
                artifact_sha256,
                parameter_sha256,
                sql_data_sha256,
            )
        };

        let digests_match = Sha256::digest(&sealed_data.artifact).as_slice() == artifact_sha256
            && Sha256::digest(&sealed_data.parameters).as_slice() == parameter_sha256
            && Sha256::digest(&sealed_data.sql_data).as_slice() == sql_data_sha256;
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
            let entry = StartQueueEntry {
                invocation_id,
                principal_id: reservation.principal_id,
                workload_class: reservation.metadata.workload_class,
                queued_at: Instant::now(),
                was_blocked: false,
            };
            let sealed_data = Arc::clone(
                reservation
                    .sealed
                    .as_ref()
                    .ok_or(StartError::Staging(StagingError::InvalidState))?,
            );
            self.enqueue_start(&mut state, entry)
                .map_err(StartError::Staging)?;
            if self.next_eligible_start(&state).map(|(_, id)| id) != Some(invocation_id) {
                mark_start_queue_blocked(&mut state, invocation_id);
                return Err(StartError::Staging(StagingError::ResourceLimit));
            }
            if let Some(entry) = take_start_queue_entry(&mut state, invocation_id) {
                record_completed_start_wait(&mut state, entry, Instant::now());
            }
            let submission = SealedSubmission {
                invocation_id,
                data: sealed_data,
                sdk_library_pin: None,
            };
            state
                .reservations
                .get_mut(&invocation_id)
                .ok_or(StartError::Staging(StagingError::Unavailable))?
                .state = ReservationState::Starting;
            submission
        };

        let mut submission = submission;
        let resolved_sdk = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.metadata_validator
                .resolve_sdk_release(&submission.data.metadata, &submission.data.artifact)
        }))
        .unwrap_or(Err(
            crate::submission_manifest::ManifestError::InvalidArtifact,
        ));
        match resolved_sdk {
            Ok(pin) => submission.sdk_library_pin = pin,
            Err(_) => {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| StartError::Staging(StagingError::Poisoned))?;
                discard_reservation(&mut state, invocation_id);
                return Err(StartError::Staging(StagingError::InvalidMetadata));
            }
        }
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

    fn enqueue_start(
        &self,
        state: &mut StagingState,
        entry: StartQueueEntry,
    ) -> Result<(), StagingError> {
        if state
            .start_queue
            .values()
            .any(|queued| queued.invocation_id == entry.invocation_id)
        {
            return Ok(());
        }
        let ticket = state.next_start_ticket;
        state.next_start_ticket = ticket.checked_add(1).ok_or(StagingError::ResourceLimit)?;
        state.start_queue.insert(ticket, entry);
        Ok(())
    }

    fn next_eligible_start(&self, state: &StagingState) -> Option<(u128, [u8; 16])> {
        state.start_queue.iter().find_map(|(ticket, entry)| {
            let active_count = state
                .reservations
                .values()
                .filter(|reservation| {
                    matches!(
                        reservation.state,
                        ReservationState::Starting | ReservationState::Started
                    ) && reservation.metadata.workload_class == entry.workload_class
                })
                .count();
            let active_limit = match entry.workload_class {
                WorkloadClass::Operational => self.limits.max_active_operational,
                WorkloadClass::Analytical => self.limits.max_active_analytical,
            };
            let principal_active_count = state
                .reservations
                .values()
                .filter(|reservation| {
                    matches!(
                        reservation.state,
                        ReservationState::Starting | ReservationState::Started
                    ) && reservation.principal_id == entry.principal_id
                        && reservation.metadata.workload_class == entry.workload_class
                })
                .count();
            let principal_active_limit = match entry.workload_class {
                WorkloadClass::Operational => self.limits.max_active_operational_per_principal,
                WorkloadClass::Analytical => self.limits.max_active_analytical_per_principal,
            };
            (active_count < active_limit && principal_active_count < principal_active_limit)
                .then_some((*ticket, entry.invocation_id))
        })
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
            sdk_library_pin: None,
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
    remove_start_queue_entry(state, invocation_id);
    state
        .connection_reservations
        .remove(&reservation.connection_id);
    let reservation_size = reservation
        .artifact_bytes
        .saturating_add(reservation.parameter_bytes)
        .saturating_add(reservation.sql_data_bytes)
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

fn remove_start_queue_entry(state: &mut StagingState, invocation_id: [u8; 16]) {
    let _ = take_start_queue_entry(state, invocation_id);
}

fn take_start_queue_entry(
    state: &mut StagingState,
    invocation_id: [u8; 16],
) -> Option<StartQueueEntry> {
    let ticket = state
        .start_queue
        .iter()
        .find_map(|(ticket, entry)| (entry.invocation_id == invocation_id).then_some(*ticket));
    ticket.and_then(|ticket| state.start_queue.remove(&ticket))
}

fn mark_start_queue_blocked(state: &mut StagingState, invocation_id: [u8; 16]) {
    if let Some(entry) = state
        .start_queue
        .values_mut()
        .find(|entry| entry.invocation_id == invocation_id)
    {
        entry.was_blocked = true;
    }
}

fn record_completed_start_wait(state: &mut StagingState, entry: StartQueueEntry, now: Instant) {
    if !entry.was_blocked {
        return;
    }
    let elapsed_us = u64::try_from(now.saturating_duration_since(entry.queued_at).as_micros())
        .unwrap_or(u64::MAX);
    record_start_wait_sample(state, entry.workload_class, elapsed_us);
}

fn record_start_wait_sample(
    state: &mut StagingState,
    workload_class: WorkloadClass,
    elapsed_us: u64,
) {
    let bucket = if elapsed_us <= 1_000 {
        0
    } else if elapsed_us <= 5_000 {
        1
    } else if elapsed_us <= 10_000 {
        2
    } else if elapsed_us <= 50_000 {
        3
    } else if elapsed_us <= 100_000 {
        4
    } else {
        5
    };
    let buckets = match workload_class {
        WorkloadClass::Operational => &mut state.operational_start_wait_buckets,
        WorkloadClass::Analytical => &mut state.analytical_start_wait_buckets,
    };
    buckets[bucket] = buckets[bucket].saturating_add(1);
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

    struct RejectSdkResolution;

    struct RepositorySdkResolver {
        repository: Arc<crate::sdk_registry::SdkLibraryRepository>,
    }

    impl SubmissionMetadataValidator for RepositorySdkResolver {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, _metadata: &SubmissionMetadata) -> bool {
            true
        }

        fn resolve_sdk_release(
            &self,
            metadata: &SubmissionMetadata,
            _artifact: &[u8],
        ) -> Result<
            Option<crate::sdk_registry::SdkLibraryPin>,
            crate::submission_manifest::ManifestError,
        > {
            let manifest = crate::submission_manifest::decode_manifest_v1(&metadata.manifest)?;
            self.repository
                .resolve(&manifest)
                .map(Some)
                .map_err(|_| crate::submission_manifest::ManifestError::UnavailableDependency)
        }
    }

    impl SubmissionMetadataValidator for RejectSdkResolution {
        fn supports_versions(&self, manifest_version: u32, scope_version: u32) -> bool {
            manifest_version == 1 && scope_version == 1
        }

        fn validate(&self, metadata: &SubmissionMetadata) -> bool {
            metadata.manifest == b"manifest"
                && metadata.requested_scope == requested_scope_fixture()
        }

        fn resolve_sdk_release(
            &self,
            _metadata: &SubmissionMetadata,
            _artifact: &[u8],
        ) -> Result<
            Option<crate::sdk_registry::SdkLibraryPin>,
            crate::submission_manifest::ManifestError,
        > {
            Err(crate::submission_manifest::ManifestError::UnavailableDependency)
        }
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
            sql_data_bytes: 0,
            sql_data_sha256: Sha256::digest([]).into(),
            deadline_ms: 30_000,
            workload_class,
        }
    }

    #[test]
    fn sealed_submission_preserves_interactive_package_binding() {
        let store = store(Arc::new(AcceptMetadata));
        let owner = make_owner(12, 19);
        let artifact = b"interactive artifact";
        let reservation = store.prepare(owner, request(artifact, b"")).unwrap();
        store
            .bind_interactive_package(owner, reservation.invocation_id, [0x31; 16], [0x41; 16])
            .unwrap();
        store
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                artifact,
            )
            .unwrap();
        store.seal(owner, reservation.invocation_id).unwrap();
        let identity = store
            .interactive_package_identity(owner, reservation.invocation_id)
            .unwrap()
            .unwrap();
        assert_eq!(identity.binding.transaction_id, [0x31; 16]);
        assert_eq!(identity.binding.package_id, [0x41; 16]);
        assert_eq!(identity.package_digest.len(), 32);
        assert_eq!(
            store.interactive_package_identity(make_owner(13, 19), reservation.invocation_id),
            Err(StagingError::Unavailable)
        );
        store
            .start_with(owner, reservation.invocation_id, |_| Ok::<_, ()>(()))
            .unwrap();
        let submission = store
            .started_submission(owner, reservation.invocation_id)
            .unwrap();
        assert_eq!(
            submission.interactive_package(),
            Some(InteractivePackageBinding {
                transaction_id: [0x31; 16],
                package_id: [0x41; 16],
            })
        );

        assert_eq!(
            store.bind_interactive_package(
                owner,
                reservation.invocation_id,
                [0x32; 16],
                [0x42; 16]
            ),
            Err(StagingError::InvalidState)
        );
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
        upload_and_seal_for_class(
            store,
            owner,
            artifact,
            parameters,
            WorkloadClass::Operational,
        )
    }

    fn upload_and_seal_for_class(
        store: &StagingStore,
        owner: StagingOwner,
        artifact: &[u8],
        parameters: &[u8],
        workload_class: WorkloadClass,
    ) -> [u8; 16] {
        let reservation = store
            .prepare(
                owner,
                request_for_class(artifact, parameters, workload_class),
            )
            .unwrap();
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
    fn runtime_usage_summary_reports_bounded_queue_and_reservation_totals() {
        let staging = store(Arc::new(AcceptMetadata));
        let operational_owner = make_owner(201, 202);
        let analytical_owner = make_owner(203, 204);
        let operational_id = upload_and_seal_for_class(
            &staging,
            operational_owner,
            b"one",
            b"p",
            WorkloadClass::Operational,
        );
        let analytical_id = upload_and_seal_for_class(
            &staging,
            analytical_owner,
            b"two",
            b"ab",
            WorkloadClass::Analytical,
        );
        let observed_at = Instant::now();
        {
            let mut state = staging.state.lock().unwrap();
            state.start_queue.insert(
                1,
                StartQueueEntry {
                    invocation_id: operational_id,
                    principal_id: operational_owner.principal_id(),
                    workload_class: WorkloadClass::Operational,
                    queued_at: observed_at
                        .checked_sub(Duration::from_micros(2_000))
                        .unwrap(),
                    was_blocked: false,
                },
            );
            state.start_queue.insert(
                2,
                StartQueueEntry {
                    invocation_id: analytical_id,
                    principal_id: analytical_owner.principal_id(),
                    workload_class: WorkloadClass::Analytical,
                    queued_at: observed_at
                        .checked_sub(Duration::from_micros(4_000))
                        .unwrap(),
                    was_blocked: false,
                },
            );
        }
        assert_eq!(
            staging.runtime_usage_summary_at(observed_at).unwrap(),
            StagingRuntimeUsage {
                pending_submissions: 2,
                starting_submissions: 0,
                started_submissions: 0,
                queued_operational_starts: 1,
                queued_analytical_starts: 1,
                oldest_queued_start_wait_us: 4_000,
                operational_start_wait_buckets: [0; 6],
                analytical_start_wait_buckets: [0; 6],
                reserved_submission_bytes: 73,
            }
        );

        let started_store = store(Arc::new(AcceptMetadata));
        let started_owner = make_owner(205, 206);
        let started_id = upload_and_seal(&started_store, started_owner, b"started", b"");
        started_store
            .start_with(started_owner, started_id, |_| Ok::<(), ()>(()))
            .unwrap();
        let usage = started_store.runtime_usage_summary().unwrap();
        assert_eq!(usage.pending_submissions, 0);
        assert_eq!(usage.starting_submissions, 0);
        assert_eq!(usage.started_submissions, 1);
        assert_eq!(usage.queued_operational_starts, 0);
        assert_eq!(usage.queued_analytical_starts, 0);
    }

    #[test]
    fn completed_start_wait_buckets_use_bounded_class_specific_histograms() {
        let observed_at = Instant::now();
        let mut state = StagingState::default();
        let operational_boundaries = [
            1_000, 1_001, 5_000, 5_001, 10_000, 10_001, 50_000, 50_001, 100_000, 100_001,
        ];
        for elapsed_us in operational_boundaries {
            record_start_wait_sample(&mut state, WorkloadClass::Operational, elapsed_us);
        }
        record_start_wait_sample(&mut state, WorkloadClass::Analytical, 5_001);
        record_completed_start_wait(
            &mut state,
            StartQueueEntry {
                invocation_id: [21; 16],
                principal_id: [31; 16],
                workload_class: WorkloadClass::Operational,
                queued_at: observed_at,
                was_blocked: false,
            },
            observed_at,
        );
        state.operational_start_wait_buckets[0] = u64::MAX;
        record_completed_start_wait(
            &mut state,
            StartQueueEntry {
                invocation_id: [22; 16],
                principal_id: [30; 16],
                workload_class: WorkloadClass::Operational,
                queued_at: observed_at - Duration::from_micros(1_000),
                was_blocked: true,
            },
            observed_at,
        );
        assert_eq!(
            state.operational_start_wait_buckets,
            [u64::MAX, 2, 2, 2, 2, 1]
        );
        assert_eq!(state.analytical_start_wait_buckets, [0, 0, 1, 0, 0, 0]);
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
    fn sdk_resolution_failure_discards_prepared_work_before_admission_callback() {
        let store = store(Arc::new(RejectSdkResolution));
        let owner = make_owner(27, 28);
        let invocation_id = upload_and_seal(&store, owner, b"artifact", b"params");
        let mut admission_called = false;

        assert!(matches!(
            store.start_with(owner, invocation_id, |_| {
                admission_called = true;
                Ok::<_, ()>(())
            }),
            Err(StartError::Staging(StagingError::InvalidMetadata))
        ));
        assert!(!admission_called);
        assert_eq!(
            store.status(owner, invocation_id),
            Err(StagingError::Unavailable)
        );
    }

    #[test]
    fn repository_sdk_pin_is_held_from_admission_until_finish() {
        use crate::submission_manifest::{ArtifactManifestV1, encode_manifest_v1};
        use std::collections::HashSet;
        use std::fs;
        use std::process::Command;

        let root = std::env::temp_dir().join(format!(
            "prdb-supervisor-sdk-pin-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&root).unwrap();
        let workspace = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();
        let build_dir = root.join("build");
        let output = Command::new("make")
            .current_dir(workspace)
            .args([
                "-f",
                "engine/sdk/Makefile",
                &format!("BUILD_DIR={}", build_dir.display()),
                "SDK_RUNTIME_PROFILE=test-runtime",
                "sdk-shared-release",
            ])
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let release = build_dir.join("releases/linux-x86_64/x86_64-v1/test-runtime/sdk-1-abis1");
        let metadata = fs::read(release.join("sdk-release.json")).unwrap();
        let library = fs::read(release.join("libprdb_sql.so.1")).unwrap();
        let allowed = HashSet::from(["libc.so.6".to_owned()]);
        let repository = Arc::new(
            crate::sdk_registry::SdkLibraryRepository::open(
                &root.join("instance/sdk-libraries"),
                "linux-x86_64".to_owned(),
                "x86_64-v1".to_owned(),
                "test-runtime".to_owned(),
                allowed.clone(),
                128 * 1024 * 1024,
            )
            .unwrap(),
        );
        repository.install(&metadata, &library, &allowed).unwrap();

        let manifest = encode_manifest_v1(&ArtifactManifestV1 {
            native_abi_version: 1,
            sdk_release: 1,
            cpu_baseline: "x86_64-v1".to_owned(),
            runtime_profile: "test-runtime".to_owned(),
            compiler_identity: "supervisor test".to_owned(),
            descriptor_symbol: "prdb_query_descriptor_v1".to_owned(),
            entry_point_symbol: "prdb_query_main_v1".to_owned(),
            dependencies: vec!["libc.so.6".to_owned(), "libprdb_sql.so.1".to_owned()],
        })
        .unwrap();
        let artifact = b"query bytes";
        let mut prepare = request(artifact, b"");
        prepare.manifest = manifest;
        prepare.artifact_bytes = artifact.len() as u64;
        prepare.artifact_sha256 = Sha256::digest(artifact).into();
        let owner = make_owner(73, 74);
        let store = store(Arc::new(RepositorySdkResolver {
            repository: Arc::clone(&repository),
        }));
        let reservation = store.prepare(owner, prepare).unwrap();
        store
            .upload_chunk(
                owner,
                reservation.invocation_id,
                ChunkKind::Artifact,
                0,
                artifact,
            )
            .unwrap();
        store.seal(owner, reservation.invocation_id).unwrap();
        store
            .start_with(owner, reservation.invocation_id, |submission| {
                let pin = submission.sdk_library_pin().unwrap();
                assert_eq!(pin.soname(), "libprdb_sql.so.1");
                assert_eq!(pin.sha256(), Sha256::digest(&library).as_slice());
                assert_eq!(
                    repository.remove_release(1).unwrap_err(),
                    crate::sdk_registry::SdkRegistryError::ReleasePinned
                );
                Ok::<_, ()>(())
            })
            .unwrap();
        store
            .finish_started(owner, reservation.invocation_id)
            .unwrap();
        assert_eq!(repository.remove_release(1).unwrap(), library.len() as u64);
        fs::remove_dir_all(root).unwrap();
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
    fn configured_global_active_start_caps_allow_64_operational_and_8_analytical() {
        let store = store(Arc::new(AcceptMetadata));
        let mut operational = Vec::new();
        let mut analytical = Vec::new();

        for connection in 1..=64u8 {
            let owner = make_owner(connection, connection);
            let invocation_id = upload_and_seal(&store, owner, b"op", b"");
            store
                .start_with(owner, invocation_id, |_| Ok::<_, ()>(()))
                .unwrap();
            operational.push((owner, invocation_id));
        }
        for connection in 101..=108u8 {
            let owner = make_owner(connection, connection);
            let reservation = store
                .prepare(
                    owner,
                    request_for_class(b"an", b"", WorkloadClass::Analytical),
                )
                .unwrap();
            store
                .upload_chunk(
                    owner,
                    reservation.invocation_id,
                    ChunkKind::Artifact,
                    0,
                    b"an",
                )
                .unwrap();
            store.seal(owner, reservation.invocation_id).unwrap();
            store
                .start_with(owner, reservation.invocation_id, |_| Ok::<_, ()>(()))
                .unwrap();
            analytical.push((owner, reservation.invocation_id));
        }

        let operational_overflow_owner = make_owner(250, 250);
        let operational_overflow_id =
            upload_and_seal(&store, operational_overflow_owner, b"op-overflow", b"");
        assert!(matches!(
            store.start_with(operational_overflow_owner, operational_overflow_id, |_| {
                Ok::<_, ()>(())
            }),
            Err(StartError::Staging(StagingError::ResourceLimit))
        ));
        assert_eq!(
            store.status(operational_overflow_owner, operational_overflow_id),
            Ok(SubmissionState::Prepared)
        );

        let analytical_overflow_owner = make_owner(251, 251);
        let analytical_overflow = store
            .prepare(
                analytical_overflow_owner,
                request_for_class(b"an-overflow", b"", WorkloadClass::Analytical),
            )
            .unwrap();
        store
            .upload_chunk(
                analytical_overflow_owner,
                analytical_overflow.invocation_id,
                ChunkKind::Artifact,
                0,
                b"an-overflow",
            )
            .unwrap();
        store
            .seal(analytical_overflow_owner, analytical_overflow.invocation_id)
            .unwrap();
        assert!(matches!(
            store.start_with(
                analytical_overflow_owner,
                analytical_overflow.invocation_id,
                |_| Ok::<_, ()>(())
            ),
            Err(StartError::Staging(StagingError::ResourceLimit))
        ));
        assert_eq!(
            store.status(analytical_overflow_owner, analytical_overflow.invocation_id),
            Ok(SubmissionState::Prepared)
        );

        for (owner, invocation_id) in operational {
            store.finish_started(owner, invocation_id).unwrap();
        }
        for (owner, invocation_id) in analytical {
            store.finish_started(owner, invocation_id).unwrap();
        }
        store
            .start_with(operational_overflow_owner, operational_overflow_id, |_| {
                Ok::<_, ()>(())
            })
            .unwrap();
        store
            .finish_started(operational_overflow_owner, operational_overflow_id)
            .unwrap();
        store
            .start_with(
                analytical_overflow_owner,
                analytical_overflow.invocation_id,
                |_| Ok::<_, ()>(()),
            )
            .unwrap();
        store
            .finish_started(analytical_overflow_owner, analytical_overflow.invocation_id)
            .unwrap();
    }

    #[test]
    fn concurrent_start_callbacks_count_toward_the_operational_cap() {
        use std::sync::mpsc;

        const ACTIVE_CAP: usize = 64;
        let store = store(Arc::new(AcceptMetadata));
        let submissions: Vec<_> = (1..=ACTIVE_CAP as u8)
            .map(|principal| {
                let owner = make_owner(principal, principal);
                let invocation_id = upload_and_seal(&store, owner, b"op", b"");
                (owner, invocation_id)
            })
            .collect();
        let overflow_owner = make_owner(250, 250);
        let overflow_id = upload_and_seal(&store, overflow_owner, b"overflow", b"");
        let start_together = Arc::new(Barrier::new(ACTIVE_CAP + 1));
        let (entered_sender, entered_receiver) = mpsc::channel();
        let (release_sender, release_receiver) = mpsc::channel();
        let release_receiver = Arc::new(Mutex::new(release_receiver));

        std::thread::scope(|scope| {
            let mut starters = Vec::with_capacity(ACTIVE_CAP);
            for (owner, invocation_id) in submissions.iter().copied() {
                let start_together = Arc::clone(&start_together);
                let entered_sender = entered_sender.clone();
                let release_receiver = Arc::clone(&release_receiver);
                let store = &store;
                starters.push(scope.spawn(move || {
                    start_together.wait();
                    store.start_with(owner, invocation_id, |_| {
                        entered_sender.send(()).unwrap();
                        release_receiver.lock().unwrap().recv().unwrap();
                        Ok::<_, ()>(())
                    })
                }));
            }

            start_together.wait();
            for _ in 0..ACTIVE_CAP {
                entered_receiver
                    .recv()
                    .expect("each admitted START callback must enter");
            }
            assert!(matches!(
                store.start_with(overflow_owner, overflow_id, |_| Ok::<_, ()>(())),
                Err(StartError::Staging(StagingError::ResourceLimit))
            ));
            assert_eq!(
                store.status(overflow_owner, overflow_id),
                Ok(SubmissionState::Prepared)
            );

            for _ in 0..ACTIVE_CAP {
                release_sender.send(()).unwrap();
            }
            for starter in starters {
                starter.join().unwrap().unwrap();
            }
        });

        for (owner, invocation_id) in submissions {
            store.finish_started(owner, invocation_id).unwrap();
        }
        store
            .start_with(overflow_owner, overflow_id, |_| Ok::<_, ()>(()))
            .unwrap();
        store.finish_started(overflow_owner, overflow_id).unwrap();
    }

    #[test]
    fn start_retries_preserve_fifo_order_within_eligible_capacity() {
        let store = StagingStore::new(
            StagingLimits {
                max_active_operational: 1,
                ..StagingLimits::default()
            },
            Arc::new(AcceptMetadata),
        )
        .unwrap();
        let first_owner = make_owner(51, 61);
        let second_owner = make_owner(52, 62);
        let third_owner = make_owner(53, 63);
        let first_id = upload_and_seal(&store, first_owner, b"first", b"");
        let second_id = upload_and_seal(&store, second_owner, b"second", b"");
        let third_id = upload_and_seal(&store, third_owner, b"third", b"");

        store
            .start_with(first_owner, first_id, |_| Ok::<_, ()>(()))
            .unwrap();
        assert!(matches!(
            store.start_with(second_owner, second_id, |_| Ok::<_, ()>(())),
            Err(StartError::Staging(StagingError::ResourceLimit))
        ));
        assert!(matches!(
            store.start_with(third_owner, third_id, |_| Ok::<_, ()>(())),
            Err(StartError::Staging(StagingError::ResourceLimit))
        ));
        store.finish_started(first_owner, first_id).unwrap();

        assert!(matches!(
            store.start_with(third_owner, third_id, |_| Ok::<_, ()>(())),
            Err(StartError::Staging(StagingError::ResourceLimit))
        ));
        assert_eq!(
            store.status(third_owner, third_id),
            Ok(SubmissionState::Prepared)
        );
        store
            .start_with(second_owner, second_id, |_| Ok::<_, ()>(()))
            .unwrap();
        store.finish_started(second_owner, second_id).unwrap();
        store
            .start_with(third_owner, third_id, |_| Ok::<_, ()>(()))
            .unwrap();
        store.finish_started(third_owner, third_id).unwrap();
        let usage = store.runtime_usage_summary().unwrap();
        assert_eq!(usage.operational_start_wait_buckets.iter().sum::<u64>(), 2);
        assert_eq!(usage.analytical_start_wait_buckets, [0; 6]);
    }

    #[test]
    fn start_queue_skips_principal_blocked_ticket_without_losing_its_turn() {
        let store = StagingStore::new(
            StagingLimits {
                max_active_operational: 2,
                max_active_operational_per_principal: 1,
                ..StagingLimits::default()
            },
            Arc::new(AcceptMetadata),
        )
        .unwrap();
        let first_owner = make_owner(57, 67);
        let same_principal_owner = make_owner(58, 67);
        let other_principal_owner = make_owner(59, 68);
        let first_id = upload_and_seal(&store, first_owner, b"first", b"");
        let same_principal_id =
            upload_and_seal(&store, same_principal_owner, b"same-principal", b"");
        let other_principal_id =
            upload_and_seal(&store, other_principal_owner, b"other-principal", b"");

        store
            .start_with(first_owner, first_id, |_| Ok::<_, ()>(()))
            .unwrap();
        assert!(matches!(
            store.start_with(same_principal_owner, same_principal_id, |_| Ok::<_, ()>(())),
            Err(StartError::Staging(StagingError::ResourceLimit))
        ));

        // The earlier ticket is blocked only by its principal's quota. The
        // later principal can use the second class slot without losing FIFO
        // priority for when the earlier ticket becomes eligible again.
        store
            .start_with(other_principal_owner, other_principal_id, |_| {
                Ok::<_, ()>(())
            })
            .unwrap();
        store.finish_started(first_owner, first_id).unwrap();
        store
            .start_with(same_principal_owner, same_principal_id, |_| Ok::<_, ()>(()))
            .unwrap();
        assert_eq!(
            store.status(same_principal_owner, same_principal_id),
            Ok(SubmissionState::Started)
        );

        store
            .finish_started(other_principal_owner, other_principal_id)
            .unwrap();
        store
            .finish_started(same_principal_owner, same_principal_id)
            .unwrap();
    }

    #[test]
    fn canceling_a_queued_start_releases_its_turn() {
        let store = StagingStore::new(
            StagingLimits {
                max_active_operational: 1,
                ..StagingLimits::default()
            },
            Arc::new(AcceptMetadata),
        )
        .unwrap();
        let first_owner = make_owner(54, 64);
        let cancelled_owner = make_owner(55, 65);
        let next_owner = make_owner(56, 66);
        let first_id = upload_and_seal(&store, first_owner, b"first", b"");
        let cancelled_id = upload_and_seal(&store, cancelled_owner, b"cancel", b"");
        let next_id = upload_and_seal(&store, next_owner, b"next", b"");

        store
            .start_with(first_owner, first_id, |_| Ok::<_, ()>(()))
            .unwrap();
        assert!(matches!(
            store.start_with(cancelled_owner, cancelled_id, |_| Ok::<_, ()>(())),
            Err(StartError::Staging(StagingError::ResourceLimit))
        ));
        store
            .cancel_prepared(cancelled_owner, cancelled_id)
            .unwrap();
        assert!(matches!(
            store.start_with(next_owner, next_id, |_| Ok::<_, ()>(())),
            Err(StartError::Staging(StagingError::ResourceLimit))
        ));

        store.finish_started(first_owner, first_id).unwrap();
        store
            .start_with(next_owner, next_id, |_| Ok::<_, ()>(()))
            .unwrap();
        store.finish_started(next_owner, next_id).unwrap();
    }

    #[test]
    fn live_reservation_quota_is_per_principal_and_released_on_cancel() {
        let store = StagingStore::new(
            StagingLimits {
                max_live_reservations_per_principal: 1,
                ..StagingLimits::default()
            },
            Arc::new(AcceptMetadata),
        )
        .unwrap();
        let owner = make_owner(51, 61);
        let same_principal = make_owner(52, 61);
        let other_principal = make_owner(53, 62);
        let first = store.prepare(owner, request(b"first", b"")).unwrap();

        assert!(matches!(
            store.prepare(same_principal, request(b"second", b"")),
            Err(StagingError::ResourceLimit)
        ));
        let other = store
            .prepare(other_principal, request(b"other", b""))
            .unwrap();
        assert_eq!(store.cancel_prepared(owner, first.invocation_id), Ok(()));
        let replacement = store
            .prepare(same_principal, request(b"replacement", b""))
            .unwrap();
        assert_eq!(
            store.cancel_prepared(other_principal, other.invocation_id),
            Ok(())
        );
        assert_eq!(
            store.cancel_prepared(same_principal, replacement.invocation_id),
            Ok(())
        );
    }

    #[test]
    fn active_invocation_quota_also_bounds_each_principal_within_a_class() {
        let store = StagingStore::new(
            StagingLimits {
                max_active_operational: 2,
                max_active_operational_per_principal: 1,
                ..StagingLimits::default()
            },
            Arc::new(AcceptMetadata),
        )
        .unwrap();
        let first_owner = make_owner(71, 81);
        let same_principal = make_owner(72, 81);
        let other_principal = make_owner(73, 82);
        let first_id = upload_and_seal(&store, first_owner, b"first", b"");
        let queued_id = upload_and_seal(&store, same_principal, b"queued", b"");
        let other_id = upload_and_seal(&store, other_principal, b"other", b"");

        store
            .start_with(first_owner, first_id, |_| Ok::<_, ()>(()))
            .unwrap();
        assert!(matches!(
            store.start_with(same_principal, queued_id, |_| Ok::<_, ()>(())),
            Err(StartError::Staging(StagingError::ResourceLimit))
        ));
        assert_eq!(
            store.status(same_principal, queued_id),
            Ok(SubmissionState::Prepared)
        );
        store
            .start_with(other_principal, other_id, |_| Ok::<_, ()>(()))
            .unwrap();
        store.finish_started(first_owner, first_id).unwrap();
        store
            .start_with(same_principal, queued_id, |_| Ok::<_, ()>(()))
            .unwrap();
        store.finish_started(same_principal, queued_id).unwrap();
        store.finish_started(other_principal, other_id).unwrap();
    }

    #[test]
    fn pending_submission_quotas_are_isolated_by_class_and_released_on_cancel() {
        let store = StagingStore::new(
            StagingLimits {
                max_pending_operational: 1,
                max_pending_analytical: 1,
                ..StagingLimits::default()
            },
            Arc::new(AcceptMetadata),
        )
        .unwrap();
        let operational = make_owner(91, 101);
        let next_operational = make_owner(92, 102);
        let analytical = make_owner(93, 103);
        let first = store.prepare(operational, request(b"op", b"")).unwrap();
        assert!(matches!(
            store.prepare(next_operational, request(b"op2", b"")),
            Err(StagingError::ResourceLimit)
        ));
        let analytical_request = store
            .prepare(
                analytical,
                request_for_class(b"an", b"", WorkloadClass::Analytical),
            )
            .unwrap();
        store
            .cancel_prepared(operational, first.invocation_id)
            .unwrap();
        let replacement = store
            .prepare(next_operational, request(b"op3", b""))
            .unwrap();
        store
            .cancel_prepared(analytical, analytical_request.invocation_id)
            .unwrap();
        store
            .cancel_prepared(next_operational, replacement.invocation_id)
            .unwrap();
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
            optimizer_path: std::path::PathBuf::from("/missing/prdb-optimizer"),
            scratch_parent: std::env::temp_dir(),
            runtime_read_execute: Vec::new(),
            runtime_read_only: Vec::new(),
            spill_broker: None,
            max_total_result_bytes: crate::query_abi::MAX_TOTAL_RESULT_BYTES,
            analytical_max_total_result_bytes: None,
            max_sql_index_probe_candidates: crate::query_abi::DEFAULT_SQL_INDEX_PROBE_CANDIDATES,
            analytical_max_sql_index_probe_candidates: None,
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
