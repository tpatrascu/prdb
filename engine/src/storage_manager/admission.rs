use super::*;
use redb::Database;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

pub(super) const TOKEN_REQUEST_WINDOW: Duration = Duration::from_secs(60);
pub(super) const MAX_TRACKED_TOKEN_REQUEST_ACTORS: usize = 4096;
pub(super) const MAX_AUTH_FAILURE_AUDITS_PER_TOKEN_PER_MINUTE: usize = 4;
pub(super) const SUPERVISOR_IDLE_POLL_INTERVAL: Duration = Duration::from_millis(100);
pub const MAX_OWN_INVOCATION_PAGE: usize = 128;
pub const MAX_ADMIN_INVOCATION_PAGE: usize = 128;
pub const MAX_ADMIN_AUDIT_PAGE: usize = 128;
/// Maximum number of schema records returned by one authenticated discovery
/// snapshot. Clients can request a fresh snapshot when this bound is reached.
pub const MAX_DISCOVERED_SCHEMAS: usize = 4096;
pub(super) const MAX_DISCOVERED_SCHEMA_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_RELATIONS: usize = 4096;
pub(super) const MAX_DISCOVERED_RELATION_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_COLUMNS: usize = 4096;
pub(super) const MAX_DISCOVERED_COLUMN_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_INDEXES: usize = 4096;
pub(super) const MAX_DISCOVERED_INDEX_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_CONSTRAINTS: usize = 4096;
pub(super) const MAX_DISCOVERED_CONSTRAINT_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_GRANTS: usize = 4096;
pub(super) const MAX_DISCOVERED_GRANT_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_POLICIES: usize = 4096;
pub(super) const MAX_DISCOVERED_POLICY_BYTES: usize = 1024 * 1024;
pub const MAX_DISCOVERED_DEPENDENCIES: usize = 4096;
pub(super) const MAX_TABLE_TRUNCATE_ROWS: usize = 65_536;
pub(super) const MAX_TABLE_TRUNCATE_INDEX_ENTRIES: usize = 65_536;
pub(super) const MAX_TABLE_TRUNCATE_BYTES: usize = 64 * 1024 * 1024;
pub(super) static TOKEN_REQUEST_LIMITERS: OnceLock<
    Mutex<HashMap<usize, Weak<TokenRequestLimiter>>>,
> = OnceLock::new();
pub(super) static ADMIN_CANCELLATION_LIMITERS: OnceLock<
    Mutex<HashMap<usize, Weak<TokenRequestLimiter>>>,
> = OnceLock::new();
pub(super) static AUTH_FAILURE_AUDIT_LIMITERS: OnceLock<
    Mutex<HashMap<usize, Weak<TokenRequestLimiter>>>,
> = OnceLock::new();
pub(super) static INVOCATION_REGISTRIES: OnceLock<
    Mutex<HashMap<usize, Weak<InvocationAdmissionState>>>,
> = OnceLock::new();
pub(super) static UNCERTAIN_INVOCATION_REGISTRIES: OnceLock<
    Mutex<HashMap<usize, Arc<InvocationAdmissionState>>>,
> = OnceLock::new();

pub(super) struct TokenRequestLimiter {
    pub(super) limit: AtomicUsize,
    pub(super) requests: Mutex<HashMap<[u8; 16], VecDeque<Instant>>>,
}

impl TokenRequestLimiter {
    pub(super) fn new(limit: usize) -> Self {
        Self {
            limit: AtomicUsize::new(limit),
            requests: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn consume(&self, actor_id: [u8; 16]) -> Result<(), TokenLifecycleError> {
        let now = Instant::now();
        let mut requests = self
            .requests
            .lock()
            .map_err(|_| TokenLifecycleError::RateLimited)?;
        requests.retain(|_, history| {
            while history.front().is_some_and(|oldest| {
                now.saturating_duration_since(*oldest) >= TOKEN_REQUEST_WINDOW
            }) {
                history.pop_front();
            }
            !history.is_empty()
        });
        if !requests.contains_key(&actor_id) {
            if requests.len() >= MAX_TRACKED_TOKEN_REQUEST_ACTORS {
                return Err(TokenLifecycleError::RateLimited);
            }
            requests.insert(actor_id, VecDeque::new());
        }
        let history = requests
            .get_mut(&actor_id)
            .ok_or(TokenLifecycleError::RateLimited)?;
        if history.len() >= self.limit.load(Ordering::Relaxed) {
            return Err(TokenLifecycleError::RateLimited);
        }
        history.push_back(now);
        Ok(())
    }
}

pub(super) fn shared_token_request_limiter(
    database: &Database,
    configured_limit: usize,
) -> Arc<TokenRequestLimiter> {
    let database_key = database as *const Database as usize;
    let registry = TOKEN_REQUEST_LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, limiter| limiter.strong_count() != 0);
    if let Some(limiter) = registry.get(&database_key).and_then(Weak::upgrade) {
        limiter.limit.fetch_min(configured_limit, Ordering::Relaxed);
        return limiter;
    }
    let limiter = Arc::new(TokenRequestLimiter::new(configured_limit));
    registry.insert(database_key, Arc::downgrade(&limiter));
    limiter
}

pub(super) fn shared_admin_cancellation_limiter(
    database: &Database,
    configured_limit: usize,
) -> Arc<TokenRequestLimiter> {
    let database_key = database as *const Database as usize;
    let registry = ADMIN_CANCELLATION_LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, limiter| limiter.strong_count() != 0);
    if let Some(limiter) = registry.get(&database_key).and_then(Weak::upgrade) {
        limiter.limit.fetch_min(configured_limit, Ordering::Relaxed);
        return limiter;
    }
    let limiter = Arc::new(TokenRequestLimiter::new(configured_limit));
    registry.insert(database_key, Arc::downgrade(&limiter));
    limiter
}

pub(super) fn shared_auth_failure_audit_limiter(database: &Database) -> Arc<TokenRequestLimiter> {
    let database_key = database as *const Database as usize;
    let registry = AUTH_FAILURE_AUDIT_LIMITERS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, limiter| limiter.strong_count() != 0);
    if let Some(limiter) = registry.get(&database_key).and_then(Weak::upgrade) {
        return limiter;
    }
    let limiter = Arc::new(TokenRequestLimiter::new(
        MAX_AUTH_FAILURE_AUDITS_PER_TOKEN_PER_MINUTE,
    ));
    registry.insert(database_key, Arc::downgrade(&limiter));
    limiter
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum LiveInvocationPhase {
    Active,
    CancellationPending,
    Committing,
}

#[derive(Clone, Copy)]
pub(super) struct LiveInvocation {
    pub(super) transaction_id: [u8; 16],
    pub(super) principal_id: [u8; 16],
    pub(super) workload_class: crate::supervisor::WorkloadClass,
    pub(super) started_at_us: i64,
    pub(super) deadline_at_us: Option<i64>,
    pub(super) phase: LiveInvocationPhase,
    // Keep ambiguous commit outcomes visible as COMMITTING until a reopened
    // Database handle resolves them from redb's recovered durable state.
    pub(super) commit_uncertain: bool,
    pub(super) staged_rows: usize,
    pub(super) staged_value_bytes: usize,
    pub(super) snapshot_pin_count: u8,
    pub(super) snapshot_pinned_at: Option<Instant>,
    pub(super) cancellation_reason_code: Option<u16>,
    pub(super) owner_generation: u128,
}

pub(super) struct InvocationAdmissionState {
    pub(super) database_key: usize,
    pub(super) limit: AtomicUsize,
    pub(super) analytical_limit: AtomicUsize,
    pub(super) statistics_collection_limit: AtomicUsize,
    pub(super) statistics_collection_active: AtomicBool,
    pub(super) database_uncertain: AtomicBool,
    pub(super) live: Mutex<LiveInvocationRegistry>,
    pub(super) changed: Condvar,
}

pub(super) struct LiveInvocationRegistry {
    pub(super) generation: u128,
    pub(super) invocations_admitted_total: u64,
    pub(super) invocation_conflicts_total: u64,
    pub(super) admin_cancellations_accepted_total: u64,
    pub(super) admin_cancellations_denied_total: u64,
    pub(super) invocation_policy_denials_total: u64,
    pub(super) invocation_staged_value_bytes_total: u64,
    pub(super) invocation_commit_latency_buckets: [u64; 6],
    pub(super) invocation_redb_write_acquisition_buckets: [u64; 6],
    pub(super) outcome_maintenance_passes_total: u64,
    pub(super) outcome_maintenance_failures_total: u64,
    pub(super) outcomes_pruned_total: u64,
    pub(super) entries: HashMap<[u8; 16], LiveInvocation>,
}

pub(super) fn bounded_invocation_ids(
    ids: impl IntoIterator<Item = [u8; 16]>,
    after: Option<[u8; 16]>,
    limit: usize,
) -> Vec<[u8; 16]> {
    if limit == 0 {
        return Vec::new();
    }
    let capacity = limit.saturating_add(1);
    let mut selected = std::collections::BinaryHeap::with_capacity(capacity);
    for id in ids {
        if after.is_some_and(|after| id <= after) {
            continue;
        }
        if selected.len() < capacity {
            selected.push(id);
        } else if selected.peek().is_some_and(|largest| id < *largest) {
            selected.pop();
            selected.push(id);
        }
    }
    selected.into_sorted_vec()
}

pub(super) struct AdministrativeInvocationProjection {
    pub(super) invocations: Vec<AdministrativeInvocationStatus>,
    pub(super) continuation: Option<AdministrativeInvocationCursor>,
    pub(super) pinned_snapshot_count: usize,
    pub(super) oldest_snapshot_pin_age_us: u64,
    pub(super) invocations_admitted_total: u64,
    pub(super) invocation_conflicts_total: u64,
    pub(super) admin_cancellations_accepted_total: u64,
    pub(super) admin_cancellations_denied_total: u64,
    pub(super) invocation_policy_denials_total: u64,
    pub(super) invocation_staged_value_bytes_total: u64,
    pub(super) invocation_commit_latency_buckets: [u64; 6],
    pub(super) invocation_redb_write_acquisition_buckets: [u64; 6],
    pub(super) outcome_maintenance_passes_total: u64,
    pub(super) outcome_maintenance_failures_total: u64,
    pub(super) outcomes_pruned_total: u64,
}

impl LiveInvocationRegistry {
    pub(super) fn record_admin_cancellation(&mut self) {
        self.admin_cancellations_accepted_total =
            self.admin_cancellations_accepted_total.saturating_add(1);
    }

    pub(super) fn record_admin_cancellation_denial(&mut self) {
        self.admin_cancellations_denied_total =
            self.admin_cancellations_denied_total.saturating_add(1);
    }

    pub(super) fn record_invocation_policy_denial(&mut self) {
        self.invocation_policy_denials_total =
            self.invocation_policy_denials_total.saturating_add(1);
    }

    pub(super) fn record_invocation_staged_value_bytes(&mut self, bytes: usize) {
        let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
        self.invocation_staged_value_bytes_total = self
            .invocation_staged_value_bytes_total
            .saturating_add(bytes);
    }

    pub(super) fn record_invocation_commit_latency(&mut self, elapsed: Duration) {
        let bucket = if elapsed <= Duration::from_micros(1_000) {
            0
        } else if elapsed <= Duration::from_micros(5_000) {
            1
        } else if elapsed <= Duration::from_micros(10_000) {
            2
        } else if elapsed <= Duration::from_micros(50_000) {
            3
        } else if elapsed <= Duration::from_micros(100_000) {
            4
        } else {
            5
        };
        self.invocation_commit_latency_buckets[bucket] =
            self.invocation_commit_latency_buckets[bucket].saturating_add(1);
    }

    pub(super) fn record_invocation_redb_write_acquisition(&mut self, elapsed: Duration) {
        let bucket = if elapsed <= Duration::from_micros(1_000) {
            0
        } else if elapsed <= Duration::from_micros(5_000) {
            1
        } else if elapsed <= Duration::from_micros(10_000) {
            2
        } else if elapsed <= Duration::from_micros(50_000) {
            3
        } else if elapsed <= Duration::from_micros(100_000) {
            4
        } else {
            5
        };
        self.invocation_redb_write_acquisition_buckets[bucket] =
            self.invocation_redb_write_acquisition_buckets[bucket].saturating_add(1);
    }

    pub(super) fn record_outcome_maintenance(
        &mut self,
        result: &Result<
            crate::outcome::OutcomePruneResult,
            crate::outcome::OutcomeMaintenanceError,
        >,
    ) {
        self.outcome_maintenance_passes_total =
            self.outcome_maintenance_passes_total.saturating_add(1);
        match result {
            Ok(result) => {
                self.outcomes_pruned_total = self
                    .outcomes_pruned_total
                    .saturating_add(u64::try_from(result.removed).unwrap_or(u64::MAX));
            }
            Err(_) => {
                self.outcome_maintenance_failures_total =
                    self.outcome_maintenance_failures_total.saturating_add(1);
            }
        }
    }

    pub(super) fn advance_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1).max(1);
    }

    pub(super) fn advance_owner_generation(&mut self, principal_id: [u8; 16]) {
        self.advance_generation();
        let generation = self.generation;
        for invocation in self.entries.values_mut() {
            if invocation.principal_id == principal_id {
                invocation.owner_generation = generation;
            }
        }
    }
}

pub(crate) struct InvocationAdmissionPermit {
    pub(super) state: Arc<InvocationAdmissionState>,
    pub(super) invocation_id: [u8; 16],
}

#[derive(Clone)]
pub(crate) struct InvocationAdmissionMetrics {
    pub(super) state: Weak<InvocationAdmissionState>,
}

impl InvocationAdmissionMetrics {
    pub(crate) fn record_policy_denial(&self) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut live = state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.record_invocation_policy_denial();
    }

    pub(crate) fn record_commit_latency(&self, elapsed: Duration) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut live = state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.record_invocation_commit_latency(elapsed);
    }

    pub(crate) fn record_write_acquisition(&self, elapsed: Duration) {
        let Some(state) = self.state.upgrade() else {
            return;
        };
        let mut live = state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.record_invocation_redb_write_acquisition(elapsed);
    }
}

impl InvocationAdmissionPermit {
    pub(crate) fn metrics(&self) -> InvocationAdmissionMetrics {
        InvocationAdmissionMetrics {
            state: Arc::downgrade(&self.state),
        }
    }

    pub(crate) fn record_staged_value_bytes(&self, bytes: usize) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.record_invocation_staged_value_bytes(bytes);
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AdmissionError {
    Limit,
    Duplicate,
    DatabaseUncertain,
    RandomnessUnavailable,
}

impl InvocationAdmissionState {
    pub(super) fn acquire(
        self: &Arc<Self>,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        workload_class: crate::supervisor::WorkloadClass,
        started_at_us: i64,
        max_age: Duration,
    ) -> Result<InvocationAdmissionPermit, AdmissionError> {
        let mut live = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.database_uncertain.load(Ordering::Acquire) {
            return Err(AdmissionError::DatabaseUncertain);
        }
        if live.entries.contains_key(&invocation_id) {
            return Err(AdmissionError::Duplicate);
        }
        if workload_class == crate::supervisor::WorkloadClass::Analytical
            && live
                .entries
                .values()
                .filter(|invocation| {
                    invocation.workload_class == crate::supervisor::WorkloadClass::Analytical
                })
                .count()
                >= self.analytical_limit.load(Ordering::Acquire)
        {
            return Err(AdmissionError::Limit);
        }
        if live.entries.len() >= self.limit.load(Ordering::Acquire) {
            return Err(AdmissionError::Limit);
        }
        let transaction_id = loop {
            let mut candidate = [0; 16];
            if getrandom::fill(&mut candidate).is_err() {
                return Err(AdmissionError::RandomnessUnavailable);
            }
            if candidate != [0; 16]
                && !live
                    .entries
                    .values()
                    .any(|entry| entry.transaction_id == candidate)
            {
                break candidate;
            }
        };
        let deadline_at_us = i64::try_from(max_age.as_micros())
            .ok()
            .and_then(|delta| started_at_us.checked_add(delta));
        live.entries.insert(
            invocation_id,
            LiveInvocation {
                transaction_id,
                principal_id,
                workload_class,
                started_at_us,
                deadline_at_us,
                phase: LiveInvocationPhase::Active,
                commit_uncertain: false,
                staged_rows: 0,
                staged_value_bytes: 0,
                snapshot_pin_count: 0,
                snapshot_pinned_at: None,
                cancellation_reason_code: None,
                owner_generation: 0,
            },
        );
        live.invocations_admitted_total = live.invocations_admitted_total.saturating_add(1);
        live.advance_owner_generation(principal_id);
        Ok(InvocationAdmissionPermit {
            state: Arc::clone(self),
            invocation_id,
        })
    }

    pub(super) fn status(&self, invocation_id: [u8; 16]) -> Option<LiveInvocation> {
        self.live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entries
            .get(&invocation_id)
            .copied()
    }

    pub(super) fn own_page(
        &self,
        principal_id: [u8; 16],
        limit: usize,
        cursor: Option<&InvocationListCursor>,
    ) -> Result<(Vec<OwnInvocationStatus>, Option<InvocationListCursor>), InvocationStatusError>
    {
        let live = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let owner_generation = live
            .entries
            .values()
            .filter(|invocation| invocation.principal_id == principal_id)
            .map(|invocation| invocation.owner_generation)
            .max();
        if let Some(cursor) = cursor {
            if cursor.principal_id != principal_id {
                return Err(InvocationStatusError::InvalidCursor);
            }
            if owner_generation != Some(cursor.owner_generation) {
                return Err(InvocationStatusError::StaleCursor);
            }
        }
        let mut ids = bounded_invocation_ids(
            live.entries
                .iter()
                .filter(|(_, invocation)| invocation.principal_id == principal_id)
                .map(|(invocation_id, _)| *invocation_id),
            cursor.map(|cursor| cursor.after_invocation_id),
            limit,
        );
        let has_more = ids.len() > limit;
        ids.truncate(limit);
        let entries = ids
            .into_iter()
            .filter_map(|invocation_id| {
                let invocation = live.entries.get(&invocation_id)?;
                Some(OwnInvocationStatus {
                    invocation_id,
                    staged_rows: invocation.staged_rows,
                    staged_value_bytes: invocation.staged_value_bytes,
                    status: match invocation.phase {
                        LiveInvocationPhase::Active => InvocationStatus::Active {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: invocation.cancellation_reason_code.is_some(),
                        },
                        LiveInvocationPhase::CancellationPending => InvocationStatus::Active {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: false,
                        },
                        LiveInvocationPhase::Committing => InvocationStatus::Committing {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: invocation.cancellation_reason_code.is_some(),
                        },
                    },
                })
            })
            .collect::<Vec<_>>();
        let continuation = if has_more {
            let owner_generation = owner_generation.ok_or(InvocationStatusError::InvalidRequest)?;
            entries.last().map(|entry| InvocationListCursor {
                principal_id,
                owner_generation,
                after_invocation_id: entry.invocation_id,
            })
        } else {
            None
        };
        Ok((entries, continuation))
    }

    pub(super) fn administrative_page(
        &self,
        administrator_id: [u8; 16],
        now_us: i64,
        observed_at: Instant,
        limit: usize,
        cursor: Option<&AdministrativeInvocationCursor>,
    ) -> Result<AdministrativeInvocationProjection, AdministrativeInvocationListError> {
        let live = self
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(cursor) = cursor {
            if cursor.administrator_id != administrator_id {
                return Err(AdministrativeInvocationListError::InvalidCursor);
            }
            if cursor.registry_generation != live.generation {
                return Err(AdministrativeInvocationListError::StaleCursor);
            }
        }
        let pinned_snapshot_count = live
            .entries
            .values()
            .filter(|invocation| invocation.snapshot_pin_count != 0)
            .count();
        let oldest_snapshot_pin_age_us = live
            .entries
            .values()
            .filter_map(|invocation| invocation.snapshot_pinned_at)
            .map(|pinned_at| {
                u64::try_from(observed_at.saturating_duration_since(pinned_at).as_micros())
                    .unwrap_or(u64::MAX)
            })
            .max()
            .unwrap_or(0);
        let mut ids = bounded_invocation_ids(
            live.entries.keys().copied(),
            cursor.map(|cursor| cursor.after_invocation_id),
            limit,
        );
        let has_more = ids.len() > limit;
        ids.truncate(limit);
        let entries = ids
            .into_iter()
            .filter_map(|invocation_id| {
                let invocation = live.entries.get(&invocation_id)?;
                Some(AdministrativeInvocationStatus {
                    invocation_id,
                    transaction_id: invocation.transaction_id,
                    principal_id: invocation.principal_id,
                    workload_class: invocation.workload_class,
                    status: match invocation.phase {
                        LiveInvocationPhase::Active => InvocationStatus::Active {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: invocation.cancellation_reason_code.is_some(),
                        },
                        LiveInvocationPhase::CancellationPending => InvocationStatus::Active {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: false,
                        },
                        LiveInvocationPhase::Committing => InvocationStatus::Committing {
                            started_at_us: invocation.started_at_us,
                            deadline_at_us: invocation.deadline_at_us,
                            cancellation_requested: invocation.cancellation_reason_code.is_some(),
                        },
                    },
                    age_us: u64::try_from(now_us.saturating_sub(invocation.started_at_us))
                        .unwrap_or(0),
                    staged_rows: invocation.staged_rows,
                    staged_value_bytes: invocation.staged_value_bytes,
                    snapshot_pin_count: invocation.snapshot_pin_count,
                    cancellation_reason_code: invocation.cancellation_reason_code,
                })
            })
            .collect::<Vec<_>>();
        let continuation = if has_more {
            entries.last().map(|entry| AdministrativeInvocationCursor {
                administrator_id,
                registry_generation: live.generation,
                after_invocation_id: entry.invocation_id,
            })
        } else {
            None
        };
        Ok(AdministrativeInvocationProjection {
            invocations: entries,
            continuation,
            pinned_snapshot_count,
            oldest_snapshot_pin_age_us,
            invocations_admitted_total: live.invocations_admitted_total,
            invocation_conflicts_total: live.invocation_conflicts_total,
            admin_cancellations_accepted_total: live.admin_cancellations_accepted_total,
            admin_cancellations_denied_total: live.admin_cancellations_denied_total,
            invocation_policy_denials_total: live.invocation_policy_denials_total,
            invocation_staged_value_bytes_total: live.invocation_staged_value_bytes_total,
            invocation_commit_latency_buckets: live.invocation_commit_latency_buckets,
            invocation_redb_write_acquisition_buckets: live
                .invocation_redb_write_acquisition_buckets,
            outcome_maintenance_passes_total: live.outcome_maintenance_passes_total,
            outcome_maintenance_failures_total: live.outcome_maintenance_failures_total,
            outcomes_pruned_total: live.outcomes_pruned_total,
        })
    }
}

impl InvocationAdmissionPermit {
    pub(crate) fn update_snapshot_pin_count(&self, snapshot_pin_count: u8) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(invocation) = live.entries.get_mut(&self.invocation_id)
            && invocation.snapshot_pin_count != snapshot_pin_count
        {
            invocation.snapshot_pin_count = snapshot_pin_count;
            invocation.snapshot_pinned_at = if snapshot_pin_count == 0 {
                None
            } else {
                Some(Instant::now())
            };
            live.advance_generation();
        }
    }

    pub(crate) fn record_conflict(&self) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.invocation_conflicts_total = live.invocation_conflicts_total.saturating_add(1);
    }

    pub(crate) fn update_write_set_usage(&self, staged_rows: usize, staged_value_bytes: usize) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let principal_id = live
            .entries
            .get(&self.invocation_id)
            .and_then(|invocation| {
                (invocation.staged_rows != staged_rows
                    || invocation.staged_value_bytes != staged_value_bytes)
                    .then_some(invocation.principal_id)
            });
        if let Some(principal_id) = principal_id {
            let invocation = live
                .entries
                .get_mut(&self.invocation_id)
                .expect("invocation was observed while the registry lock was held");
            invocation.staged_rows = staged_rows;
            invocation.staged_value_bytes = staged_value_bytes;
            live.advance_owner_generation(principal_id);
        }
    }

    pub(crate) fn mark_commit_uncertain(&self) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.state.database_uncertain.store(true, Ordering::Release);
        if let Some(invocation) = live.entries.get_mut(&self.invocation_id) {
            invocation.phase = LiveInvocationPhase::Committing;
            invocation.commit_uncertain = true;
            let principal_id = invocation.principal_id;
            live.advance_owner_generation(principal_id);
        }
        self.state.changed.notify_all();
        retain_uncertain_registry(self.state.database_key, Arc::clone(&self.state));
    }

    pub(crate) fn may_continue(&self) -> bool {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            let phase = live
                .entries
                .get(&self.invocation_id)
                .map(|invocation| (invocation.phase, invocation.cancellation_reason_code));
            match phase {
                Some((LiveInvocationPhase::Active, None)) => return true,
                Some((LiveInvocationPhase::CancellationPending, _)) => {
                    live = self
                        .state
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                _ => return false,
            }
        }
    }

    pub(crate) fn cancellation_requested(&self) -> bool {
        let live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.entries
            .get(&self.invocation_id)
            .is_some_and(|invocation| invocation.cancellation_reason_code.is_some())
    }

    pub(crate) fn try_mark_committing(&self) -> bool {
        self.try_mark_committing_then(|| {})
    }

    pub(super) fn try_mark_committing_then(&self, after_transition: impl FnOnce()) -> bool {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut after_transition = Some(after_transition);
        loop {
            let phase = live
                .entries
                .get(&self.invocation_id)
                .map(|invocation| (invocation.phase, invocation.cancellation_reason_code));
            match phase {
                Some((LiveInvocationPhase::Active, None)) => {
                    let Some(invocation) = live.entries.get_mut(&self.invocation_id) else {
                        return false;
                    };
                    invocation.phase = LiveInvocationPhase::Committing;
                    let principal_id = invocation.principal_id;
                    live.advance_owner_generation(principal_id);
                    self.state.changed.notify_all();
                    drop(live);
                    after_transition
                        .take()
                        .expect("commit transition hook is consumed once")();
                    return true;
                }
                Some((LiveInvocationPhase::CancellationPending, _)) => {
                    live = self
                        .state
                        .changed
                        .wait(live)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                }
                _ => return false,
            }
        }
    }
}

impl Drop for InvocationAdmissionPermit {
    fn drop(&mut self) {
        let mut live = self
            .state
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let should_remove = live
            .entries
            .get(&self.invocation_id)
            .is_some_and(|invocation| !invocation.commit_uncertain);
        if should_remove && let Some(invocation) = live.entries.remove(&self.invocation_id) {
            live.advance_owner_generation(invocation.principal_id);
            self.state.changed.notify_all();
        }
    }
}

pub(super) fn invocation_registry(
    database: &Database,
    configured_limit: usize,
    configured_analytical_limit: usize,
    configured_statistics_collection_limit: usize,
) -> Arc<InvocationAdmissionState> {
    let database_key = database as *const Database as usize;
    let uncertain = UNCERTAIN_INVOCATION_REGISTRIES.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(invocations) = uncertain
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .get(&database_key)
        .cloned()
    {
        invocations
            .limit
            .fetch_min(configured_limit, Ordering::Relaxed);
        invocations
            .analytical_limit
            .fetch_min(configured_analytical_limit, Ordering::Relaxed);
        invocations
            .statistics_collection_limit
            .fetch_min(configured_statistics_collection_limit, Ordering::Relaxed);
        return invocations;
    }
    let registry = INVOCATION_REGISTRIES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    registry.retain(|_, invocations| invocations.strong_count() != 0);
    if let Some(invocations) = registry.get(&database_key).and_then(Weak::upgrade) {
        invocations
            .limit
            .fetch_min(configured_limit, Ordering::Relaxed);
        invocations
            .analytical_limit
            .fetch_min(configured_analytical_limit, Ordering::Relaxed);
        invocations
            .statistics_collection_limit
            .fetch_min(configured_statistics_collection_limit, Ordering::Relaxed);
        return invocations;
    }
    let invocations = Arc::new(InvocationAdmissionState {
        database_key,
        limit: AtomicUsize::new(configured_limit),
        analytical_limit: AtomicUsize::new(configured_analytical_limit),
        statistics_collection_limit: AtomicUsize::new(configured_statistics_collection_limit),
        statistics_collection_active: AtomicBool::new(false),
        database_uncertain: AtomicBool::new(false),
        live: Mutex::new(LiveInvocationRegistry {
            generation: 1,
            invocations_admitted_total: 0,
            invocation_conflicts_total: 0,
            admin_cancellations_accepted_total: 0,
            admin_cancellations_denied_total: 0,
            invocation_policy_denials_total: 0,
            invocation_staged_value_bytes_total: 0,
            invocation_commit_latency_buckets: [0; 6],
            invocation_redb_write_acquisition_buckets: [0; 6],
            outcome_maintenance_passes_total: 0,
            outcome_maintenance_failures_total: 0,
            outcomes_pruned_total: 0,
            entries: HashMap::new(),
        }),
        changed: Condvar::new(),
    });
    registry.insert(database_key, Arc::downgrade(&invocations));
    invocations
}

pub(super) fn shared_invocation_registry(
    database: &Database,
    configured_limit: usize,
    configured_analytical_limit: usize,
    configured_statistics_collection_limit: usize,
) -> Arc<InvocationAdmissionState> {
    invocation_registry(
        database,
        configured_limit,
        configured_analytical_limit,
        configured_statistics_collection_limit,
    )
}

pub(super) fn retain_uncertain_registry(database_key: usize, state: Arc<InvocationAdmissionState>) {
    UNCERTAIN_INVOCATION_REGISTRIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(database_key, state);
}

pub(super) fn has_live_invocation_registry(database: &Database) -> bool {
    let database_key = database as *const Database as usize;
    if UNCERTAIN_INVOCATION_REGISTRIES
        .get()
        .and_then(|registries| registries.lock().ok())
        .is_some_and(|registries| registries.contains_key(&database_key))
    {
        return true;
    }
    let Some(registries) = INVOCATION_REGISTRIES.get() else {
        return false;
    };
    let Ok(mut registries) = registries.lock() else {
        return true;
    };
    registries.retain(|_, registry| registry.strong_count() != 0);
    registries
        .get(&database_key)
        .is_some_and(|registry| registry.strong_count() != 0)
}

/// Quarantine this database handle after redb reports a commit error whose
/// publication state requires close/reopen recovery.
pub(crate) fn mark_database_uncertain(database: &Database) {
    let invocations = invocation_registry(
        database,
        StorageManagerConfig::default().max_active_invocations,
        StorageManagerConfig::default().max_active_analytical_invocations,
        StorageManagerConfig::default().max_statistics_collection_rows,
    );
    let first_uncertain = !invocations.database_uncertain.swap(true, Ordering::AcqRel);
    if first_uncertain {
        let mut live = invocations
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        live.generation = live.generation.wrapping_add(1).max(1);
    }
    retain_uncertain_registry(invocations.database_key, invocations);
}

/// Forget the closed handle's quarantined process-local state after a fresh
/// redb handle has been opened and validated.
pub(crate) fn clear_uncertain_database_key(database_key: usize) {
    UNCERTAIN_INVOCATION_REGISTRIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&database_key);
    INVOCATION_REGISTRIES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .remove(&database_key);
}
