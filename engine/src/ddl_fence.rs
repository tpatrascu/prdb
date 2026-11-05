//! Per-table coordination between data writers and schema-changing work.
//!
//! Writers acquire shared fences for every table they can modify. DDL acquires
//! exclusive fences for every affected table in one operation. A manager must
//! be shared by all writers and DDL operations for the same database; pinned
//! read-only snapshots do not need a fence.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DdlFenceError {
    InvalidTableId,
    TimedOut,
    DeadlineOutOfRange,
    TooManyWaiters,
}

impl std::fmt::Display for DdlFenceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "DDL fence unavailable: {self:?}")
    }
}

impl std::error::Error for DdlFenceError {}

#[derive(Clone, Default)]
pub struct DdlFenceManager {
    inner: Arc<DdlFenceInner>,
}

/// Shares one fence manager and timeout policy across coordinated commit paths.
#[derive(Clone)]
pub struct DdlFenceCoordinator {
    manager: DdlFenceManager,
    timeout: Duration,
}

#[derive(Debug, Default)]
struct DdlFenceInner {
    state: Mutex<FenceState>,
    changed: Condvar,
}

#[derive(Debug, Default)]
struct FenceState {
    tables: HashMap<[u8; 16], TableFence>,
}

#[derive(Debug, Default)]
struct TableFence {
    active_writers: usize,
    exclusive_held: bool,
    waiting_exclusive: usize,
}

#[derive(Clone, Copy, Debug)]
enum FenceMode {
    Shared,
    Exclusive,
}

/// RAII ownership of one or more table fences.
#[must_use = "dropping the guard releases its table fences"]
#[derive(Debug)]
pub struct DdlFenceGuard {
    inner: Arc<DdlFenceInner>,
    table_ids: Vec<[u8; 16]>,
    mode: FenceMode,
}

impl DdlFenceManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Admit a data writer for all listed tables until the returned guard drops.
    pub fn acquire_shared(
        &self,
        table_ids: &[[u8; 16]],
        timeout: Duration,
    ) -> Result<DdlFenceGuard, DdlFenceError> {
        self.acquire(table_ids, timeout, FenceMode::Shared)
    }

    /// Exclude data writers for all listed tables until the returned guard drops.
    pub fn acquire_exclusive(
        &self,
        table_ids: &[[u8; 16]],
        timeout: Duration,
    ) -> Result<DdlFenceGuard, DdlFenceError> {
        self.acquire(table_ids, timeout, FenceMode::Exclusive)
    }

    fn acquire(
        &self,
        table_ids: &[[u8; 16]],
        timeout: Duration,
        mode: FenceMode,
    ) -> Result<DdlFenceGuard, DdlFenceError> {
        let table_ids = normalize_table_ids(table_ids)?;
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(DdlFenceError::DeadlineOutOfRange)?;
        let mut state = self.lock_state();
        match mode {
            FenceMode::Shared
                if table_ids.iter().any(|table_id| {
                    state
                        .tables
                        .get(table_id)
                        .is_some_and(|fence| fence.active_writers == usize::MAX)
                }) =>
            {
                return Err(DdlFenceError::TooManyWaiters);
            }
            FenceMode::Exclusive
                if table_ids.iter().any(|table_id| {
                    state
                        .tables
                        .get(table_id)
                        .is_some_and(|fence| fence.waiting_exclusive == usize::MAX)
                }) =>
            {
                return Err(DdlFenceError::TooManyWaiters);
            }
            _ => {}
        }
        if matches!(mode, FenceMode::Exclusive) {
            for table_id in &table_ids {
                state.tables.entry(*table_id).or_default().waiting_exclusive += 1;
            }
        }

        loop {
            if can_acquire(&state, &table_ids, mode) {
                for table_id in &table_ids {
                    let fence = state.tables.entry(*table_id).or_default();
                    match mode {
                        FenceMode::Shared => fence.active_writers += 1,
                        FenceMode::Exclusive => {
                            fence.waiting_exclusive -= 1;
                            fence.exclusive_held = true;
                        }
                    }
                }
                return Ok(DdlFenceGuard {
                    inner: Arc::clone(&self.inner),
                    table_ids,
                    mode,
                });
            }

            let now = Instant::now();
            if now >= deadline {
                if matches!(mode, FenceMode::Exclusive) {
                    for table_id in &table_ids {
                        if let Some(fence) = state.tables.get_mut(table_id) {
                            fence.waiting_exclusive -= 1;
                        }
                        remove_unused_fence(&mut state, table_id);
                    }
                    self.inner.changed.notify_all();
                }
                return Err(DdlFenceError::TimedOut);
            }
            let remaining = deadline.saturating_duration_since(now);
            let (next_state, _) = self
                .inner
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(PoisonError::into_inner);
            state = next_state;
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, FenceState> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

impl DdlFenceCoordinator {
    pub fn new(manager: DdlFenceManager, timeout: Duration) -> Self {
        Self { manager, timeout }
    }

    pub fn acquire_shared(&self, table_ids: &[[u8; 16]]) -> Result<DdlFenceGuard, DdlFenceError> {
        self.manager.acquire_shared(table_ids, self.timeout)
    }

    pub fn acquire_exclusive(
        &self,
        table_ids: &[[u8; 16]],
    ) -> Result<DdlFenceGuard, DdlFenceError> {
        self.manager.acquire_exclusive(table_ids, self.timeout)
    }
}

impl Drop for DdlFenceGuard {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for table_id in &self.table_ids {
            if let Some(fence) = state.tables.get_mut(table_id) {
                match self.mode {
                    FenceMode::Shared => fence.active_writers -= 1,
                    FenceMode::Exclusive => fence.exclusive_held = false,
                }
            }
            remove_unused_fence(&mut state, table_id);
        }
        drop(state);
        self.inner.changed.notify_all();
    }
}

fn normalize_table_ids(table_ids: &[[u8; 16]]) -> Result<Vec<[u8; 16]>, DdlFenceError> {
    if table_ids.contains(&[0; 16]) {
        return Err(DdlFenceError::InvalidTableId);
    }
    let mut normalized = table_ids.to_vec();
    normalized.sort_unstable();
    normalized.dedup();
    Ok(normalized)
}

fn can_acquire(state: &FenceState, table_ids: &[[u8; 16]], mode: FenceMode) -> bool {
    table_ids.iter().all(|table_id| {
        let fence = state.tables.get(table_id);
        match mode {
            FenceMode::Shared => {
                fence.is_none_or(|fence| !fence.exclusive_held && fence.waiting_exclusive == 0)
            }
            FenceMode::Exclusive => {
                fence.is_none_or(|fence| !fence.exclusive_held && fence.active_writers == 0)
            }
        }
    })
}

fn remove_unused_fence(state: &mut FenceState, table_id: &[u8; 16]) {
    if state.tables.get(table_id).is_some_and(|fence| {
        fence.active_writers == 0 && !fence.exclusive_held && fence.waiting_exclusive == 0
    }) {
        state.tables.remove(table_id);
    }
}
