use super::admission::*;
use super::catalog_helpers::*;
use super::*;
use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::outcome::{InvocationOutcome, lookup_outcome};
use crate::schema::{AUDIT, CATALOG, OUTCOMES};
use crate::snapshot::{MAX_CATALOG_FAMILY_SCAN, RowSnapshot};
use redb::{Database, ReadableDatabase, ReadableTable, ReadableTableMetadata};
use std::collections::{HashMap, HashSet};
use std::time::Instant;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InvocationStatusConsistency {
    BestEffort,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvocationStatus {
    Active {
        started_at_us: i64,
        deadline_at_us: Option<i64>,
        cancellation_requested: bool,
    },
    Committing {
        started_at_us: i64,
        deadline_at_us: Option<i64>,
        cancellation_requested: bool,
    },
    Terminal(InvocationOutcome),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationStatusSnapshot {
    pub observed_at_us: i64,
    pub consistency: InvocationStatusConsistency,
    pub status: InvocationStatus,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OwnInvocationStatus {
    pub invocation_id: [u8; 16],
    pub status: InvocationStatus,
    pub staged_rows: usize,
    pub staged_value_bytes: usize,
}

/// Opaque continuation for one owner's live invocation page sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationListCursor {
    pub(super) principal_id: [u8; 16],
    pub(super) owner_generation: u128,
    pub(super) after_invocation_id: [u8; 16],
}

impl InvocationListCursor {
    /// Serialize the owner-bound live-invocation cursor as lowercase hex.
    pub fn to_wire(&self) -> String {
        let mut bytes = [0; 49];
        bytes[0] = 1;
        bytes[1..17].copy_from_slice(&self.principal_id);
        bytes[17..33].copy_from_slice(&self.owner_generation.to_be_bytes());
        bytes[33..49].copy_from_slice(&self.after_invocation_id);
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
            output.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
        }
        output
    }

    /// Parse a canonical wire cursor. The owner and generation are checked
    /// again by `list_own_invocations` on every request.
    pub fn from_wire(value: &str) -> Option<Self> {
        if value.len() != 98 {
            return None;
        }
        let mut bytes = [0; 49];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = lower_hex_value(pair[0])?;
            let low = lower_hex_value(pair[1])?;
            bytes[index] = high << 4 | low;
        }
        if bytes[0] != 1 {
            return None;
        }
        let principal_id = bytes[1..17].try_into().ok()?;
        let owner_generation = u128::from_be_bytes(bytes[17..33].try_into().ok()?);
        let after_invocation_id = bytes[33..49].try_into().ok()?;
        if principal_id == [0; 16] || owner_generation == 0 || after_invocation_id == [0; 16] {
            return None;
        }
        Some(Self {
            principal_id,
            owner_generation,
            after_invocation_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InvocationStatusPage {
    pub observed_at_us: i64,
    pub consistency: InvocationStatusConsistency,
    pub invocations: Vec<OwnInvocationStatus>,
    pub continuation: Option<InvocationListCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeInvocationStatus {
    pub invocation_id: [u8; 16],
    pub transaction_id: [u8; 16],
    pub principal_id: [u8; 16],
    pub status: InvocationStatus,
    pub age_us: u64,
    pub staged_rows: usize,
    pub staged_value_bytes: usize,
    pub snapshot_pin_count: u8,
    pub cancellation_reason_code: Option<u16>,
}

/// Opaque continuation for an administrator's live runtime page sequence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeInvocationCursor {
    pub(super) administrator_id: [u8; 16],
    pub(super) registry_generation: u128,
    pub(super) after_invocation_id: [u8; 16],
}

impl AdministrativeInvocationCursor {
    /// Serialize the process-local page cursor as lowercase hexadecimal.
    pub fn to_wire(&self) -> String {
        let mut bytes = [0; 49];
        bytes[0] = 1;
        bytes[1..17].copy_from_slice(&self.administrator_id);
        bytes[17..33].copy_from_slice(&self.registry_generation.to_be_bytes());
        bytes[33..49].copy_from_slice(&self.after_invocation_id);
        let mut output = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            output.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
            output.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
        }
        output
    }

    /// Parse a canonical wire cursor. Authorization and generation freshness
    /// are still checked by `list_runtime_invocations` on every page request.
    pub fn from_wire(value: &str) -> Option<Self> {
        if value.len() != 98 {
            return None;
        }
        let mut bytes = [0; 49];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = lower_hex_value(pair[0])?;
            let low = lower_hex_value(pair[1])?;
            bytes[index] = high << 4 | low;
        }
        if bytes[0] != 1 {
            return None;
        }
        let administrator_id = bytes[1..17].try_into().ok()?;
        let registry_generation = u128::from_be_bytes(bytes[17..33].try_into().ok()?);
        let after_invocation_id = bytes[33..49].try_into().ok()?;
        if administrator_id == [0; 16] || registry_generation == 0 || after_invocation_id == [0; 16]
        {
            return None;
        }
        Some(Self {
            administrator_id,
            registry_generation,
            after_invocation_id,
        })
    }
}

pub(super) fn lower_hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeInvocationPage {
    pub observed_at_us: i64,
    pub consistency: InvocationStatusConsistency,
    pub invocations: Vec<AdministrativeInvocationStatus>,
    pub pinned_snapshot_count: usize,
    pub oldest_snapshot_pin_age_us: u64,
    pub invocations_admitted_total: u64,
    pub invocation_conflicts_total: u64,
    pub admin_cancellations_accepted_total: u64,
    pub admin_cancellations_denied_total: u64,
    pub invocation_policy_denials_total: u64,
    pub invocation_staged_value_bytes_total: u64,
    pub invocation_commit_latency_buckets: [u64; 6],
    pub invocation_redb_write_acquisition_buckets: [u64; 6],
    pub outcome_maintenance_passes_total: u64,
    pub outcome_maintenance_failures_total: u64,
    pub outcomes_pruned_total: u64,
    pub fence_waits: Vec<crate::ddl_fence::DdlFenceWaitStatus>,
    pub fence_waits_truncated: bool,
    pub continuation: Option<AdministrativeInvocationCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InvocationStatusError {
    InvalidRequest,
    InvalidPageLimit,
    InvalidCursor,
    StaleCursor,
    Storage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OwnerCancellationResult {
    Accepted,
    TooLate,
    Terminal(InvocationOutcome),
    Unknown,
}

impl std::fmt::Display for InvocationStatusError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invocation status lookup failed: {self:?}")
    }
}

impl std::error::Error for InvocationStatusError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdministrativeInvocationListError {
    InvalidRequest,
    InvalidPageLimit,
    InvalidCursor,
    StaleCursor,
    Unauthorized,
    Storage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeAuditCursor {
    pub(super) administrator_id: [u8; 16],
    pub(super) after_event_id: [u8; 16],
}

impl AdministrativeAuditCursor {
    pub fn to_wire(&self) -> String {
        let mut output = String::with_capacity(66);
        for byte in std::iter::once(1)
            .chain(self.administrator_id)
            .chain(self.after_event_id)
        {
            output.push(char::from(b"0123456789abcdef"[(byte >> 4) as usize]));
            output.push(char::from(b"0123456789abcdef"[(byte & 0x0f) as usize]));
        }
        output
    }

    pub fn from_wire(value: &str) -> Option<Self> {
        if value.len() != 66 {
            return None;
        }
        let mut bytes = [0; 33];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            let high = lower_hex_value(pair[0])?;
            let low = lower_hex_value(pair[1])?;
            bytes[index] = high << 4 | low;
        }
        if bytes[0] != 1 {
            return None;
        }
        let administrator_id = bytes[1..17].try_into().ok()?;
        let after_event_id = bytes[17..33].try_into().ok()?;
        (administrator_id != [0; 16] && after_event_id != [0; 16]).then_some(Self {
            administrator_id,
            after_event_id,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeAuditEvent {
    pub event_id: [u8; 16],
    pub actor_id: Option<[u8; 16]>,
    pub correlation_id: [u8; 16],
    pub occurred_at_us: i64,
    pub action: AuditAction,
    pub result: AuditResult,
    pub target_ids: Vec<[u8; 16]>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdministrativeAuditPage {
    pub events: Vec<AdministrativeAuditEvent>,
    pub continuation: Option<AdministrativeAuditCursor>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdministrativeAuditListError {
    InvalidRequest,
    InvalidPageLimit,
    InvalidCursor,
    Unauthorized,
    Storage,
}

impl std::fmt::Display for AdministrativeAuditListError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "administrative audit listing failed: {self:?}")
    }
}

impl std::error::Error for AdministrativeAuditListError {}

impl std::fmt::Display for AdministrativeInvocationListError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "administrative invocation listing failed: {self:?}"
        )
    }
}

impl std::error::Error for AdministrativeInvocationListError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdministrativeCancellationResult {
    Accepted,
    TooLate,
    Terminal(InvocationOutcome),
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AdministrativeCancellationError {
    InvalidRequest,
    InvalidAudit,
    Unauthorized,
    RateLimited,
    AuditCapacityExceeded,
    Storage,
}

impl std::fmt::Display for AdministrativeCancellationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "administrative cancellation failed: {self:?}")
    }
}

impl std::error::Error for AdministrativeCancellationError {}

impl<'db> StorageManager<'db> {
    /// Resolve a retained terminal outcome for an already authenticated owner.
    /// Unknown, mismatched, and expired records all return `None`.
    pub fn lookup_invocation_outcome(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        now_us: i64,
    ) -> Result<Option<InvocationOutcome>, Box<dyn std::error::Error>> {
        if self.requires_reopen_after_commit_error() {
            return Err(std::io::Error::other(
                "database requires reopen after an ambiguous commit error",
            )
            .into());
        }
        lookup_outcome(
            self.database,
            invocation_id,
            authenticated_principal_id,
            now_us,
        )
    }

    /// Return live state from this process registry or a retained durable
    /// outcome. Callers must authenticate the principal before this lookup;
    /// unknown IDs, expired outcomes, and other owners all return `None`.
    pub fn get_invocation_status(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        now_us: i64,
    ) -> Result<Option<InvocationStatusSnapshot>, InvocationStatusError> {
        if invocation_id == [0; 16] || authenticated_principal_id == [0; 16] || now_us <= 0 {
            return Err(InvocationStatusError::InvalidRequest);
        }
        if let Some(live) = self.admissions.status(invocation_id) {
            if live.principal_id != authenticated_principal_id {
                return Ok(None);
            }
            if self.requires_reopen_after_commit_error() && !live.commit_uncertain {
                return Err(InvocationStatusError::Storage);
            }
            let status = match live.phase {
                LiveInvocationPhase::Active => InvocationStatus::Active {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: live.cancellation_reason_code.is_some(),
                },
                LiveInvocationPhase::CancellationPending => InvocationStatus::Active {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: false,
                },
                LiveInvocationPhase::Committing => InvocationStatus::Committing {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: live.cancellation_reason_code.is_some(),
                },
            };
            return Ok(Some(InvocationStatusSnapshot {
                observed_at_us: now_us,
                consistency: InvocationStatusConsistency::BestEffort,
                status,
            }));
        }
        if self.requires_reopen_after_commit_error() {
            return Err(InvocationStatusError::Storage);
        }
        lookup_outcome(
            self.database,
            invocation_id,
            authenticated_principal_id,
            now_us,
        )
        .map_err(|_| InvocationStatusError::Storage)
        .map(|outcome| {
            outcome.map(|outcome| InvocationStatusSnapshot {
                observed_at_us: now_us,
                consistency: InvocationStatusConsistency::BestEffort,
                status: InvocationStatus::Terminal(outcome),
            })
        })
    }

    /// Return one invocation's live or retained terminal status to a current
    /// database administrator. Authorization is checked on a fresh catalog
    /// snapshot before the invocation ID is inspected.
    pub fn get_administrative_invocation_status(
        &self,
        authenticated_administrator_id: [u8; 16],
        invocation_id: [u8; 16],
        now_us: i64,
    ) -> Result<Option<InvocationStatusSnapshot>, AdministrativeInvocationListError> {
        if authenticated_administrator_id == [0; 16] || invocation_id == [0; 16] || now_us <= 0 {
            return Err(AdministrativeInvocationListError::InvalidRequest);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        let snapshot = RowSnapshot::new(read);
        let is_administrator = database_admin_authority(&snapshot, authenticated_administrator_id)
            .map_err(|error| match error {
                TokenLifecycleError::TokenUnavailable => {
                    AdministrativeInvocationListError::Unauthorized
                }
                _ => AdministrativeInvocationListError::Storage,
            })?;
        drop(snapshot);
        if !is_administrator {
            return Err(AdministrativeInvocationListError::Unauthorized);
        }

        if self.requires_reopen_after_commit_error() {
            return match self.admissions.status(invocation_id) {
                Some(live) if live.commit_uncertain => Ok(Some(InvocationStatusSnapshot {
                    observed_at_us: now_us,
                    consistency: InvocationStatusConsistency::BestEffort,
                    status: InvocationStatus::Committing {
                        started_at_us: live.started_at_us,
                        deadline_at_us: live.deadline_at_us,
                        cancellation_requested: live.cancellation_reason_code.is_some(),
                    },
                })),
                _ => Err(AdministrativeInvocationListError::Storage),
            };
        }

        if let Some(live) = self.admissions.status(invocation_id) {
            let status = match live.phase {
                LiveInvocationPhase::Active | LiveInvocationPhase::CancellationPending => {
                    InvocationStatus::Active {
                        started_at_us: live.started_at_us,
                        deadline_at_us: live.deadline_at_us,
                        cancellation_requested: live.cancellation_reason_code.is_some(),
                    }
                }
                LiveInvocationPhase::Committing => InvocationStatus::Committing {
                    started_at_us: live.started_at_us,
                    deadline_at_us: live.deadline_at_us,
                    cancellation_requested: live.cancellation_reason_code.is_some(),
                },
            };
            return Ok(Some(InvocationStatusSnapshot {
                observed_at_us: now_us,
                consistency: InvocationStatusConsistency::BestEffort,
                status,
            }));
        }

        let read = self
            .database
            .begin_read()
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        let bytes = read
            .open_table(OUTCOMES)
            .map_err(|_| AdministrativeInvocationListError::Storage)?
            .get(&invocation_id[..])
            .map_err(|_| AdministrativeInvocationListError::Storage)?
            .map(|guard| guard.value().to_vec());
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let outcome = InvocationOutcome::decode(&bytes)
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        if outcome.invocation_id != invocation_id {
            return Err(AdministrativeInvocationListError::Storage);
        }
        if now_us >= outcome.retention_deadline_us {
            return Ok(None);
        }
        Ok(Some(InvocationStatusSnapshot {
            observed_at_us: now_us,
            consistency: InvocationStatusConsistency::BestEffort,
            status: InvocationStatus::Terminal(outcome),
        }))
    }

    /// Request cancellation for an invocation owned by the authenticated
    /// principal. The commit transition and cancellation request are
    /// serialized by the admission registry; unknown and foreign IDs match.
    pub fn cancel_owned_invocation(
        &self,
        authenticated_principal_id: [u8; 16],
        invocation_id: [u8; 16],
        now_us: i64,
    ) -> Result<OwnerCancellationResult, InvocationStatusError> {
        if authenticated_principal_id == [0; 16] || invocation_id == [0; 16] || now_us <= 0 {
            return Err(InvocationStatusError::InvalidRequest);
        }
        loop {
            let mut live = self
                .admissions
                .live
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let invocation = live.entries.get(&invocation_id).copied();
            match invocation {
                Some(invocation) if invocation.principal_id != authenticated_principal_id => {
                    return Ok(OwnerCancellationResult::Unknown);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Committing => {
                    return Ok(OwnerCancellationResult::TooLate);
                }
                Some(invocation)
                    if invocation.phase == LiveInvocationPhase::CancellationPending =>
                {
                    live = self
                        .admissions
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    drop(live);
                }
                Some(invocation) if invocation.cancellation_reason_code.is_some() => {
                    return Ok(OwnerCancellationResult::Accepted);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Active => {
                    if let Some(entry) = live.entries.get_mut(&invocation_id) {
                        entry.phase = LiveInvocationPhase::CancellationPending;
                    }
                    drop(live);
                    let mut live = self
                        .admissions
                        .live
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let Some(entry) = live.entries.get_mut(&invocation_id) else {
                        self.admissions.changed.notify_all();
                        drop(live);
                        continue;
                    };
                    if entry.phase == LiveInvocationPhase::CancellationPending {
                        entry.phase = LiveInvocationPhase::Active;
                        entry.cancellation_reason_code = Some(1);
                        live.advance_owner_generation(authenticated_principal_id);
                    }
                    self.admissions.changed.notify_all();
                    return Ok(OwnerCancellationResult::Accepted);
                }
                None => {
                    drop(live);
                    return match lookup_outcome(
                        self.database,
                        invocation_id,
                        authenticated_principal_id,
                        now_us,
                    )
                    .map_err(|_| InvocationStatusError::Storage)?
                    {
                        Some(outcome) => Ok(OwnerCancellationResult::Terminal(outcome)),
                        None => Ok(OwnerCancellationResult::Unknown),
                    };
                }
                Some(_) => return Ok(OwnerCancellationResult::TooLate),
            }
        }
    }

    /// Return a bounded best-effort page of this authenticated owner's live
    /// invocations. Terminal outcomes remain available through point status.
    /// A change to this owner's entries invalidates an outstanding
    /// continuation so pages never silently skip or repeat this owner's rows.
    pub fn list_own_invocations(
        &self,
        authenticated_principal_id: [u8; 16],
        now_us: i64,
        page_limit: usize,
        continuation: Option<&InvocationListCursor>,
    ) -> Result<InvocationStatusPage, InvocationStatusError> {
        if authenticated_principal_id == [0; 16] || now_us <= 0 {
            return Err(InvocationStatusError::InvalidRequest);
        }
        if page_limit == 0 || page_limit > MAX_OWN_INVOCATION_PAGE {
            return Err(InvocationStatusError::InvalidPageLimit);
        }
        if continuation.is_some_and(|cursor| {
            cursor.principal_id == [0; 16] || cursor.after_invocation_id == [0; 16]
        }) {
            return Err(InvocationStatusError::InvalidCursor);
        }
        let (invocations, continuation) =
            self.admissions
                .own_page(authenticated_principal_id, page_limit, continuation)?;
        Ok(InvocationStatusPage {
            observed_at_us: now_us,
            consistency: InvocationStatusConsistency::BestEffort,
            invocations,
            continuation,
        })
    }

    /// Return a bounded best-effort page of live invocations for a current
    /// database administrator. Authority is revalidated from the latest
    /// catalog snapshot on every call; no global total is returned.
    pub fn list_runtime_invocations(
        &self,
        authenticated_administrator_id: [u8; 16],
        now_us: i64,
        page_limit: usize,
        continuation: Option<&AdministrativeInvocationCursor>,
    ) -> Result<AdministrativeInvocationPage, AdministrativeInvocationListError> {
        if authenticated_administrator_id == [0; 16] || now_us <= 0 {
            return Err(AdministrativeInvocationListError::InvalidRequest);
        }
        if page_limit == 0 || page_limit > MAX_ADMIN_INVOCATION_PAGE {
            return Err(AdministrativeInvocationListError::InvalidPageLimit);
        }
        if continuation.is_some_and(|cursor| {
            cursor.administrator_id == [0; 16] || cursor.after_invocation_id == [0; 16]
        }) {
            return Err(AdministrativeInvocationListError::InvalidCursor);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| AdministrativeInvocationListError::Storage)?;
        let snapshot = RowSnapshot::new(read);
        let is_administrator = database_admin_authority(&snapshot, authenticated_administrator_id)
            .map_err(|error| match error {
                TokenLifecycleError::TokenUnavailable => {
                    AdministrativeInvocationListError::Unauthorized
                }
                _ => AdministrativeInvocationListError::Storage,
            })?;
        drop(snapshot);
        if !is_administrator {
            return Err(AdministrativeInvocationListError::Unauthorized);
        }
        let observed_at = Instant::now();
        let projection = self.admissions.administrative_page(
            authenticated_administrator_id,
            now_us,
            observed_at,
            page_limit,
            continuation,
        )?;
        let (fence_waits, fence_waits_truncated) = self.ddl_fences.wait_snapshot();
        Ok(AdministrativeInvocationPage {
            observed_at_us: now_us,
            consistency: InvocationStatusConsistency::BestEffort,
            invocations: projection.invocations,
            pinned_snapshot_count: projection.pinned_snapshot_count,
            oldest_snapshot_pin_age_us: projection.oldest_snapshot_pin_age_us,
            invocations_admitted_total: projection.invocations_admitted_total,
            invocation_conflicts_total: projection.invocation_conflicts_total,
            admin_cancellations_accepted_total: projection.admin_cancellations_accepted_total,
            admin_cancellations_denied_total: projection.admin_cancellations_denied_total,
            invocation_policy_denials_total: projection.invocation_policy_denials_total,
            invocation_staged_value_bytes_total: projection.invocation_staged_value_bytes_total,
            invocation_commit_latency_buckets: projection.invocation_commit_latency_buckets,
            invocation_redb_write_acquisition_buckets: projection
                .invocation_redb_write_acquisition_buckets,
            outcome_maintenance_passes_total: projection.outcome_maintenance_passes_total,
            outcome_maintenance_failures_total: projection.outcome_maintenance_failures_total,
            outcomes_pruned_total: projection.outcomes_pruned_total,
            fence_waits,
            fence_waits_truncated,
            continuation: projection.continuation,
        })
    }

    /// Return a bounded page of durable AUD1 records in exclusive event-ID
    /// order. The cursor is bound to the administrator and authority is
    /// checked against the same redb snapshot used to read the audit table.
    pub fn list_audit_events(
        &self,
        authenticated_administrator_id: [u8; 16],
        page_limit: usize,
        continuation: Option<&AdministrativeAuditCursor>,
    ) -> Result<AdministrativeAuditPage, AdministrativeAuditListError> {
        if authenticated_administrator_id == [0; 16] {
            return Err(AdministrativeAuditListError::InvalidRequest);
        }
        if page_limit == 0 || page_limit > MAX_ADMIN_AUDIT_PAGE {
            return Err(AdministrativeAuditListError::InvalidPageLimit);
        }
        if continuation.is_some_and(|cursor| {
            cursor.administrator_id != authenticated_administrator_id
                || cursor.after_event_id == [0; 16]
        }) {
            return Err(AdministrativeAuditListError::InvalidCursor);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|_| AdministrativeAuditListError::Storage)?;
        let snapshot = RowSnapshot::new(read);
        let is_administrator = database_admin_authority(&snapshot, authenticated_administrator_id)
            .map_err(|error| match error {
                TokenLifecycleError::TokenUnavailable => AdministrativeAuditListError::Unauthorized,
                _ => AdministrativeAuditListError::Storage,
            })?;
        if !is_administrator {
            return Err(AdministrativeAuditListError::Unauthorized);
        }
        let audit = snapshot
            .read_transaction()
            .open_table(AUDIT)
            .map_err(|_| AdministrativeAuditListError::Storage)?;
        let mut events = Vec::with_capacity(page_limit);
        let mut has_more = false;
        let first_event_id = [0; 16];
        let lower_bound = continuation
            .map(|cursor| std::ops::Bound::Excluded(cursor.after_event_id.as_slice()))
            .unwrap_or(std::ops::Bound::Included(first_event_id.as_slice()));
        let entries = audit
            .range::<&[u8]>((lower_bound, std::ops::Bound::Unbounded))
            .map_err(|_| AdministrativeAuditListError::Storage)?;
        for entry in entries {
            let (key, value) = entry.map_err(|_| AdministrativeAuditListError::Storage)?;
            let key: [u8; 16] = key
                .value()
                .try_into()
                .map_err(|_| AdministrativeAuditListError::Storage)?;
            let event = AuditEvent::decode(value.value())
                .map_err(|_| AdministrativeAuditListError::Storage)?;
            if event.event_id != key {
                return Err(AdministrativeAuditListError::Storage);
            }
            if events.len() == page_limit {
                has_more = true;
                break;
            }
            events.push(AdministrativeAuditEvent {
                event_id: event.event_id,
                actor_id: event.actor_id,
                correlation_id: event.correlation_id,
                occurred_at_us: event.occurred_at_us,
                action: event.action,
                result: event.result,
                target_ids: event.target_ids,
            });
        }
        let continuation = has_more.then(|| AdministrativeAuditCursor {
            administrator_id: authenticated_administrator_id,
            after_event_id: events.last().expect("nonempty page has more").event_id,
        });
        Ok(AdministrativeAuditPage {
            events,
            continuation,
        })
    }

    /// Request audited cancellation for a live invocation. A short-lived
    /// pending state makes commit wait for the durable audit decision without
    /// holding the registry lock during redb I/O.
    pub fn cancel_invocation(
        &self,
        authenticated_administrator_id: [u8; 16],
        invocation_id: [u8; 16],
        reason_code: u16,
        now_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<AdministrativeCancellationResult, AdministrativeCancellationError> {
        if authenticated_administrator_id == [0; 16]
            || invocation_id == [0; 16]
            || reason_code == 0
            || now_us <= 0
        {
            return Err(AdministrativeCancellationError::InvalidRequest);
        }
        self.admin_cancellation_requests
            .consume(authenticated_administrator_id)
            .map_err(|_| AdministrativeCancellationError::RateLimited)?;
        if audit_event.actor_id != Some(authenticated_administrator_id)
            || audit_event.action != AuditAction::AdministrativeCancellation
            || audit_event.target_ids.as_slice() != [invocation_id]
        {
            return Err(AdministrativeCancellationError::InvalidAudit);
        }

        loop {
            let mut live = self
                .admissions
                .live
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let invocation = live.entries.get(&invocation_id).copied();
            match invocation {
                Some(invocation)
                    if invocation.phase == LiveInvocationPhase::CancellationPending =>
                {
                    live = self
                        .admissions
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    drop(live);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Committing => {
                    drop(live);
                    audit_event.result = AuditResult::Denied;
                    persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    )?;
                    self.admissions
                        .live
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .record_admin_cancellation_denial();
                    return Ok(AdministrativeCancellationResult::TooLate);
                }
                Some(invocation) if invocation.cancellation_reason_code.is_some() => {
                    drop(live);
                    audit_event.result = AuditResult::Accepted;
                    persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    )?;
                    return Ok(AdministrativeCancellationResult::Accepted);
                }
                Some(invocation) if invocation.phase == LiveInvocationPhase::Active => {
                    if let Some(entry) = live.entries.get_mut(&invocation_id) {
                        entry.phase = LiveInvocationPhase::CancellationPending;
                    }
                    drop(live);

                    audit_event.result = AuditResult::Accepted;
                    let audit_result = persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    );

                    let mut live = self
                        .admissions
                        .live
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let mut changed = false;
                    if let Some(entry) = live.entries.get_mut(&invocation_id)
                        && entry.phase == LiveInvocationPhase::CancellationPending
                    {
                        entry.phase = LiveInvocationPhase::Active;
                        if audit_result.is_ok() {
                            entry.cancellation_reason_code = Some(reason_code);
                            changed = true;
                        }
                    }
                    if changed {
                        live.record_admin_cancellation();
                        live.advance_owner_generation(invocation.principal_id);
                    }
                    self.admissions.changed.notify_all();
                    audit_result?;
                    return Ok(AdministrativeCancellationResult::Accepted);
                }
                None => {
                    drop(live);
                    audit_event.result = AuditResult::Denied;
                    persist_admin_cancellation_audit(
                        self.database,
                        authenticated_administrator_id,
                        &audit_event,
                    )?;
                    self.admissions
                        .live
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .record_admin_cancellation_denial();
                    return match lookup_any_outcome(self.database, invocation_id, now_us)? {
                        Some(outcome) => Ok(AdministrativeCancellationResult::Terminal(outcome)),
                        None => Ok(AdministrativeCancellationResult::Unknown),
                    };
                }
                Some(_) => return Ok(AdministrativeCancellationResult::TooLate),
            }
        }
    }
}

pub(super) fn persist_admin_cancellation_audit(
    database: &Database,
    administrator_id: [u8; 16],
    event: &AuditEvent,
) -> Result<(), AdministrativeCancellationError> {
    let storage_error = || {
        mark_database_uncertain(database);
        AdministrativeCancellationError::Storage
    };
    if event.actor_id != Some(administrator_id)
        || event.action != AuditAction::AdministrativeCancellation
        || !matches!(event.result, AuditResult::Accepted | AuditResult::Denied)
    {
        return Err(AdministrativeCancellationError::InvalidAudit);
    }
    let encoded = event
        .encode()
        .map_err(|_| AdministrativeCancellationError::InvalidAudit)?;

    let read = database.begin_read().map_err(|_| storage_error())?;
    let snapshot = RowSnapshot::new(read);
    let is_administrator =
        database_admin_authority(&snapshot, administrator_id).map_err(|error| match error {
            TokenLifecycleError::TokenUnavailable => AdministrativeCancellationError::Unauthorized,
            _ => storage_error(),
        })?;
    if !is_administrator {
        return Err(AdministrativeCancellationError::Unauthorized);
    }

    let membership_records = snapshot
        .scan_catalog_family(11, MAX_CATALOG_FAMILY_SCAN)
        .map_err(|_| storage_error())?;
    let mut guarded_values = HashMap::<Vec<u8>, Vec<u8>>::new();
    let mut principal_ids = HashSet::from([administrator_id]);
    for record in membership_records {
        let key = record.logical_key;
        if key.len() != 36 || key[2] != 1 || key[19] != 1 {
            return Err(storage_error());
        }
        let member_id: [u8; 16] = key[3..19].try_into().map_err(|_| storage_error())?;
        let role_id: [u8; 16] = key[20..36].try_into().map_err(|_| storage_error())?;
        principal_ids.insert(member_id);
        principal_ids.insert(role_id);
        let value = snapshot
            .get_catalog_value(&key)
            .map_err(|_| storage_error())?
            .ok_or_else(&storage_error)?;
        guarded_values.insert(key, value);
    }
    for principal_id in principal_ids {
        let key = principal_catalog_key(principal_id);
        let value = snapshot
            .get_catalog_value(&key)
            .map_err(|_| storage_error())?
            .ok_or_else(&storage_error)?;
        guarded_values.insert(key, value);
    }
    drop(snapshot);

    let mut write = database.begin_write().map_err(|_| storage_error())?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|_| storage_error())?;
    write.set_two_phase_commit(true);
    {
        let catalog = write.open_table(CATALOG).map_err(|_| storage_error())?;
        for (key, expected) in guarded_values {
            let current = catalog
                .get(key.as_slice())
                .map_err(|_| storage_error())?
                .map(|value| value.value().to_vec());
            if current.as_deref() != Some(expected.as_slice()) {
                return Err(AdministrativeCancellationError::Unauthorized);
            }
        }
    }
    {
        let mut audit = write.open_table(AUDIT).map_err(|_| storage_error())?;
        let current_events = usize::try_from(audit.len().map_err(|_| storage_error())?)
            .map_err(|_| AdministrativeCancellationError::AuditCapacityExceeded)?;
        if !crate::audit::has_audit_capacity(current_events, 1) {
            return Err(AdministrativeCancellationError::AuditCapacityExceeded);
        }
        if audit
            .get(event.event_id.as_slice())
            .map_err(|_| storage_error())?
            .is_some()
        {
            return Err(AdministrativeCancellationError::InvalidAudit);
        }
        audit
            .insert(event.event_id.as_slice(), encoded.as_slice())
            .map_err(|_| storage_error())?;
    }
    match write.commit() {
        Ok(()) => Ok(()),
        Err(_) => Err(storage_error()),
    }
}

pub(super) fn lookup_any_outcome(
    database: &Database,
    invocation_id: [u8; 16],
    now_us: i64,
) -> Result<Option<InvocationOutcome>, AdministrativeCancellationError> {
    let read = database
        .begin_read()
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    let outcomes = read
        .open_table(OUTCOMES)
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    let Some(value) = outcomes
        .get(invocation_id.as_slice())
        .map_err(|_| AdministrativeCancellationError::Storage)?
    else {
        return Ok(None);
    };
    let outcome = InvocationOutcome::decode(value.value())
        .map_err(|_| AdministrativeCancellationError::Storage)?;
    if outcome.invocation_id != invocation_id {
        return Err(AdministrativeCancellationError::Storage);
    }
    Ok((now_us < outcome.retention_deadline_us).then_some(outcome))
}
