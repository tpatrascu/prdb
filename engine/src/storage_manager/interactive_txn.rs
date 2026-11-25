use super::supervisor::*;
use super::*;
use crate::authorization::Operation;
use crate::commit::record_aborted_outcome;
use crate::credentials::{TOKEN_SECRET_BYTES, now_unix_micros};
use crate::outcome::lookup_outcome;
use crate::schema::{ENGINE_META, INTERACTIVE_TXN_RECOVERY_MAGIC, INTERACTIVE_TXN_RECOVERY_PREFIX};
use crate::transaction::{InvocationTransaction, TransactionError};
use redb::{Database, ReadableDatabase, ReadableTable, TableHandle};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const INTERACTIVE_TRANSACTION_MAX_PACKAGES: usize = 128;
pub const INTERACTIVE_TRANSACTION_MAX_ROWS: usize = 65_536;
pub const INTERACTIVE_TRANSACTION_MAX_WRITE_BYTES: usize = 64 * 1024 * 1024;
pub const INTERACTIVE_TRANSACTION_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);
pub const MAX_RETAINED_INTERACTIVE_TRANSACTION_RECORDS: usize = 4096;
pub(super) const MAX_DISCOVERED_DEPENDENCY_BYTES: usize = 1024 * 1024;
pub(super) type InteractiveTransactionMap<'db> =
    Arc<Mutex<HashMap<[u8; 16], Arc<Mutex<InteractiveTransactionEntry<'db>>>>>>;
pub(super) type InteractiveBeginRequestMap =
    Arc<Mutex<HashMap<([u8; 16], [u8; 16]), InteractiveBeginRequestRecord>>>;

#[derive(Clone, Copy)]
pub(super) struct InteractiveBeginRequestRecord {
    transaction_id: [u8; 16],
    retain_until_us: i64,
}

pub(super) struct InteractiveTransactionEntry<'db> {
    pub(super) transaction_id: [u8; 16],
    pub(super) controller_id: [u8; 16],
    pub(super) outcome_retention_deadline_us: i64,
    pub(super) transaction: Arc<Mutex<Option<InvocationTransaction<'db>>>>,
    pub(super) state: InteractiveTransactionState,
    pub(super) started_at: Instant,
    pub(super) last_activity: Instant,
    pub(super) packages: HashMap<[u8; 16], InteractivePackageRecord>,
}

pub(super) struct InteractivePackageRecord {
    pub(super) digest: [u8; 32],
    pub(super) state: InteractivePackageState,
    pub(super) result_lost: bool,
    pub(super) actor: Option<InteractivePackageActorRecord>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct InteractivePackageActorRecord {
    token_id: [u8; 16],
    principal_id: [u8; 16],
    principal_name: String,
    authority_epoch: u64,
    role_ids: HashSet<[u8; 16]>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InteractivePackageState {
    Running,
    Completed,
    Failed,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InteractivePackageStatus {
    pub state: InteractivePackageState,
    pub result_lost: bool,
}

pub struct InteractivePackageRequest<'a> {
    pub token_id: [u8; 16],
    pub token_secret: &'a [u8; TOKEN_SECRET_BYTES],
    pub transaction_id: [u8; 16],
    pub package_id: [u8; 16],
    pub package_digest: [u8; 32],
    pub requested_scope: HashSet<(Operation, crate::authorization::AccessTarget)>,
    pub actor: Option<InteractivePackageActorProof<'a>>,
}

#[derive(Clone, Copy)]
pub struct InteractivePackageActorProof<'a> {
    pub expected_name: &'a str,
    pub token_id: [u8; 16],
    pub token_secret: &'a [u8; TOKEN_SECRET_BYTES],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InteractiveTransactionState {
    Active,
    AbortOnly,
    Committing,
    Committed,
    Aborted,
    Expired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InteractiveTransactionBegin {
    pub transaction_id: [u8; 16],
    pub state: InteractiveTransactionState,
    pub idle_timeout_ms: u64,
    pub lifetime_ms: u64,
    pub max_package_bytes: u64,
    pub max_packages: u64,
}

#[derive(Debug)]
pub enum InteractiveTransactionError {
    Transaction(TransactionError),
    InvalidRequest,
    NotFound,
    NotOwner,
    Expired,
    AbortOnly,
    PackageLimit,
    DuplicatePackage,
    PackageConflict,
    InvalidActor,
    ConcurrentOperation,
    RegistryLimit,
    RegistryPoisoned,
}

impl From<TransactionError> for InteractiveTransactionError {
    fn from(error: TransactionError) -> Self {
        Self::Transaction(error)
    }
}

impl std::fmt::Display for InteractiveTransactionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "interactive transaction error: {self:?}")
    }
}

impl std::error::Error for InteractiveTransactionError {}

#[derive(Debug)]
pub enum InteractivePackageExecutionError<E> {
    Transaction(InteractiveTransactionError),
    Execution(E),
    Panicked,
}

pub(super) fn interactive_recovery_key(transaction_id: [u8; 16]) -> Vec<u8> {
    let mut key = Vec::with_capacity(INTERACTIVE_TXN_RECOVERY_PREFIX.len() + transaction_id.len());
    key.extend_from_slice(INTERACTIVE_TXN_RECOVERY_PREFIX);
    key.extend_from_slice(&transaction_id);
    key
}

pub(super) fn persist_interactive_transaction_marker(
    database: &Database,
    transaction_id: [u8; 16],
    controller_id: [u8; 16],
    retention_deadline_us: i64,
) -> Result<(), String> {
    if transaction_id == [0; 16] || controller_id == [0; 16] || retention_deadline_us <= 0 {
        return Err("invalid interactive transaction recovery marker".to_owned());
    }
    let key = interactive_recovery_key(transaction_id);
    let mut value = [0; 28];
    value[..4].copy_from_slice(INTERACTIVE_TXN_RECOVERY_MAGIC);
    value[4..20].copy_from_slice(&controller_id);
    value[20..].copy_from_slice(&retention_deadline_us.to_le_bytes());
    let mut write = database.begin_write().map_err(|error| error.to_string())?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    write.set_two_phase_commit(true);
    {
        let mut metadata = write
            .open_table(ENGINE_META)
            .map_err(|error| error.to_string())?;
        if metadata
            .get(key.as_slice())
            .map_err(|error| error.to_string())?
            .is_some()
        {
            return Err("duplicate interactive transaction recovery marker".to_owned());
        }
        metadata
            .insert(key.as_slice(), value.as_slice())
            .map_err(|error| error.to_string())?;
    }
    write.commit().map_err(|error| error.to_string())
}

pub(super) fn remove_interactive_transaction_marker(
    database: &Database,
    transaction_id: [u8; 16],
) -> Result<(), String> {
    let key = interactive_recovery_key(transaction_id);
    let mut write = database.begin_write().map_err(|error| error.to_string())?;
    write
        .set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    write.set_two_phase_commit(true);
    write
        .open_table(ENGINE_META)
        .map_err(|error| error.to_string())?
        .remove(key.as_slice())
        .map_err(|error| error.to_string())?;
    write.commit().map_err(|error| error.to_string())
}

pub(super) fn recover_interactive_transaction_markers(database: &Database) -> Result<(), String> {
    let markers = {
        let read = database.begin_read().map_err(|error| error.to_string())?;
        if !read
            .list_tables()
            .map_err(|error| error.to_string())?
            .any(|table| table.name() == ENGINE_META.name())
        {
            return Ok(());
        }
        let metadata = read
            .open_table(ENGINE_META)
            .map_err(|error| error.to_string())?;
        let mut markers = Vec::new();
        for entry in metadata.iter().map_err(|error| error.to_string())? {
            let (key, value) = entry.map_err(|error| error.to_string())?;
            let key = key.value();
            if !key.starts_with(INTERACTIVE_TXN_RECOVERY_PREFIX) {
                continue;
            }
            if key.len() != INTERACTIVE_TXN_RECOVERY_PREFIX.len() + 16
                || value.value().len() != 28
                || value.value()[..4] != INTERACTIVE_TXN_RECOVERY_MAGIC[..]
            {
                return Err("invalid interactive transaction recovery marker".to_owned());
            }
            let transaction_id = key[INTERACTIVE_TXN_RECOVERY_PREFIX.len()..]
                .try_into()
                .map_err(|_| "invalid interactive transaction ID".to_owned())?;
            let controller_id = value.value()[4..20]
                .try_into()
                .map_err(|_| "invalid interactive transaction controller".to_owned())?;
            let retention_deadline_us = i64::from_le_bytes(
                value.value()[20..28]
                    .try_into()
                    .map_err(|_| "invalid interactive transaction deadline".to_owned())?,
            );
            if transaction_id == [0; 16] || controller_id == [0; 16] || retention_deadline_us <= 0 {
                return Err("invalid interactive transaction recovery marker".to_owned());
            }
            markers.push((transaction_id, controller_id, retention_deadline_us));
        }
        markers
    };
    if markers.is_empty() {
        return Ok(());
    }
    let now_us = now_unix_micros().ok_or_else(|| "system clock is unavailable".to_owned())?;
    for (transaction_id, controller_id, retention_deadline_us) in markers {
        if retention_deadline_us > now_us
            && lookup_outcome(database, transaction_id, controller_id, now_us)
                .map_err(|error| error.to_string())?
                .is_none()
        {
            record_aborted_outcome(
                database,
                transaction_id,
                controller_id,
                retention_deadline_us,
            )
            .map_err(|error| format!("could not persist recovered ABORTED outcome: {error:?}"))?;
        }
        remove_interactive_transaction_marker(database, transaction_id)?;
    }
    Ok(())
}

impl<'db> StorageManager<'db> {
    /// Open a server-owned transaction whose MVCC snapshot and write overlay
    /// stay alive across package requests. The transaction ID is generated by
    /// storage and is never treated as authentication authority.
    pub fn begin_interactive_transaction(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        outcome_retention_deadline_us: i64,
    ) -> Result<InteractiveTransactionBegin, InteractiveTransactionError> {
        let mut begin_request_id = [0; 16];
        getrandom::fill(&mut begin_request_id)
            .map_err(|_| TransactionError::RandomnessUnavailable)?;
        if begin_request_id == [0; 16] {
            return Err(TransactionError::RandomnessUnavailable.into());
        }
        self.begin_interactive_transaction_with_request_id(
            token_id,
            token_secret,
            outcome_retention_deadline_us,
            begin_request_id,
        )
    }

    /// Idempotently open or recover the transaction created for one client
    /// request. The key is scoped to the authenticated controller and retained
    /// through the corresponding outcome-retention deadline.
    pub fn begin_interactive_transaction_with_request_id(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        outcome_retention_deadline_us: i64,
        begin_request_id: [u8; 16],
    ) -> Result<InteractiveTransactionBegin, InteractiveTransactionError> {
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        if outcome_retention_deadline_us <= now_us || begin_request_id == [0; 16] {
            return Err(InteractiveTransactionError::InvalidRequest);
        }
        let _ = self.reap_interactive_transactions(now_us, 64)?;
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let mut begin_requests = self
            .interactive_begin_requests
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
        begin_requests.retain(|_, record| record.retain_until_us > now_us);
        let request_key = (session.principal_id(), begin_request_id);
        if let Some(record) = begin_requests.get(&request_key) {
            let entry = self.interactive_transaction_entry(record.transaction_id)?;
            let mut entry = self.try_lock_interactive_transaction(&entry)?;
            if entry.controller_id != session.principal_id() {
                return Err(InteractiveTransactionError::NotOwner);
            }
            self.expire_interactive_transaction_if_needed(&mut entry)?;
            entry.last_activity = Instant::now();
            return Ok(InteractiveTransactionBegin {
                transaction_id: record.transaction_id,
                state: entry.state,
                idle_timeout_ms: INTERACTIVE_TRANSACTION_IDLE_TIMEOUT
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
                lifetime_ms: self
                    .max_invocation_age
                    .as_millis()
                    .try_into()
                    .unwrap_or(u64::MAX),
                max_package_bytes: crate::supervisor::MAX_ARTIFACT_BYTES,
                max_packages: u64::try_from(INTERACTIVE_TRANSACTION_MAX_PACKAGES)
                    .unwrap_or(u64::MAX),
            });
        }
        if self
            .interactive_transactions
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
            .len()
            >= MAX_RETAINED_INTERACTIVE_TRANSACTION_RECORDS
            || begin_requests.len() >= MAX_RETAINED_INTERACTIVE_TRANSACTION_RECORDS
        {
            return Err(InteractiveTransactionError::RegistryLimit);
        }
        let mut transaction_id = [0; 16];
        getrandom::fill(&mut transaction_id)
            .map_err(|_| TransactionError::RandomnessUnavailable)?;
        if transaction_id == [0; 16] {
            return Err(TransactionError::RandomnessUnavailable.into());
        }
        let (mut transaction, _role_ids) = self.begin_token_authenticated_invocation(
            transaction_id,
            token_id,
            token_secret,
            outcome_retention_deadline_us,
            INTERACTIVE_TRANSACTION_MAX_ROWS,
            INTERACTIVE_TRANSACTION_MAX_WRITE_BYTES,
        )?;
        if let Err(error) = persist_interactive_transaction_marker(
            self.database,
            transaction_id,
            session.principal_id(),
            outcome_retention_deadline_us,
        ) {
            let _ = transaction.abort();
            return Err(TransactionError::Storage(error).into());
        }
        let now = Instant::now();
        let entry = Arc::new(Mutex::new(InteractiveTransactionEntry {
            transaction_id,
            controller_id: session.principal_id(),
            outcome_retention_deadline_us,
            transaction: Arc::new(Mutex::new(Some(transaction))),
            state: InteractiveTransactionState::Active,
            started_at: now,
            last_activity: now,
            packages: HashMap::new(),
        }));
        let mut registry = match self.interactive_transactions.lock() {
            Ok(registry) => registry,
            Err(_) => {
                if let Ok(entry) = entry.lock()
                    && let Ok(mut transaction) = entry.transaction.lock()
                    && let Some(mut transaction) = transaction.take()
                {
                    let _ = transaction.abort();
                }
                return Err(InteractiveTransactionError::RegistryPoisoned);
            }
        };
        if registry.contains_key(&transaction_id) {
            let duplicate = entry
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
            let mut owner = duplicate
                .transaction
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
            if let Some(mut transaction) = owner.take() {
                transaction.abort()?;
            }
            return Err(TransactionError::DuplicateInvocation.into());
        }
        registry.insert(transaction_id, entry);
        begin_requests.insert(
            request_key,
            InteractiveBeginRequestRecord {
                transaction_id,
                retain_until_us: outcome_retention_deadline_us,
            },
        );
        Ok(InteractiveTransactionBegin {
            transaction_id,
            state: InteractiveTransactionState::Active,
            idle_timeout_ms: INTERACTIVE_TRANSACTION_IDLE_TIMEOUT
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            lifetime_ms: self
                .max_invocation_age
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            max_package_bytes: crate::supervisor::MAX_ARTIFACT_BYTES,
            max_packages: u64::try_from(INTERACTIVE_TRANSACTION_MAX_PACKAGES).unwrap_or(u64::MAX),
        })
    }

    /// Authenticate the controller on every reattachment. A live transaction
    /// may move between TCP connections, but its ID is not a bearer token.
    pub fn attach_interactive_transaction(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
    ) -> Result<InteractiveTransactionState, InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry = self.try_lock_interactive_transaction(&entry)?;
        if session.principal_id() != entry.controller_id {
            return Err(InteractiveTransactionError::NotOwner);
        }
        self.expire_interactive_transaction_if_needed(&mut entry)?;
        match entry.state {
            InteractiveTransactionState::Active | InteractiveTransactionState::AbortOnly => {
                entry.last_activity = Instant::now();
                Ok(entry.state)
            }
            InteractiveTransactionState::Expired => Err(InteractiveTransactionError::Expired),
            _ => Err(InteractiveTransactionError::NotFound),
        }
    }

    /// Reserve and run one package under the retained transaction context.
    /// The transaction lock serializes package execution and the ID/digest
    /// record prevents a repeated package ID from executing again.
    pub fn execute_interactive_package<T>(
        &self,
        request: InteractivePackageRequest<'_>,
        execute: impl FnOnce(&mut InvocationTransaction<'db>) -> Result<T, TransactionError>,
    ) -> Result<T, InteractiveTransactionError> {
        let InteractivePackageRequest {
            token_id,
            token_secret,
            transaction_id,
            package_id,
            package_digest,
            requested_scope,
            actor,
        } = request;
        if package_id == [0; 16] || transaction_id == [0; 16] {
            return Err(InteractiveTransactionError::InvalidRequest);
        }
        let session = self.authenticate_query_session(token_id, token_secret)?;
        if actor.is_some() {
            return Err(InteractiveTransactionError::InvalidActor);
        }
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry = self.try_lock_interactive_transaction(&entry)?;
        if session.principal_id() != entry.controller_id {
            return Err(InteractiveTransactionError::NotOwner);
        }
        self.expire_interactive_transaction_if_needed(&mut entry)?;
        match entry.state {
            InteractiveTransactionState::Active => (),
            InteractiveTransactionState::AbortOnly => {
                return Err(InteractiveTransactionError::AbortOnly);
            }
            InteractiveTransactionState::Expired => {
                return Err(InteractiveTransactionError::Expired);
            }
            _ => return Err(InteractiveTransactionError::NotFound),
        }
        if entry.packages.contains_key(&package_id) {
            let same_package = entry
                .packages
                .get(&package_id)
                .is_some_and(|record| record.digest == package_digest);
            return Err(if same_package {
                InteractiveTransactionError::DuplicatePackage
            } else {
                InteractiveTransactionError::PackageConflict
            });
        }
        if entry.packages.len() >= INTERACTIVE_TRANSACTION_MAX_PACKAGES {
            return Err(InteractiveTransactionError::PackageLimit);
        }
        entry.packages.insert(
            package_id,
            InteractivePackageRecord {
                digest: package_digest,
                state: InteractivePackageState::Running,
                result_lost: false,
                actor: None,
            },
        );
        entry.last_activity = Instant::now();
        let execution = {
            let mut owner = entry
                .transaction
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
            let transaction = owner
                .as_mut()
                .ok_or(InteractiveTransactionError::NotFound)?;
            transaction.set_package_requested_scope(Some(requested_scope));
            let result = execute(transaction);
            transaction.set_package_requested_scope(None);
            result
        };
        match execution {
            Ok(result) => {
                if let Some(record) = entry.packages.get_mut(&package_id) {
                    record.state = InteractivePackageState::Completed;
                }
                entry.last_activity = Instant::now();
                Ok(result)
            }
            Err(error) => {
                if let Some(record) = entry.packages.get_mut(&package_id) {
                    record.state = InteractivePackageState::Failed;
                }
                entry.state = InteractiveTransactionState::AbortOnly;
                let abort_result = entry
                    .transaction
                    .lock()
                    .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
                    .as_mut()
                    .ok_or(InteractiveTransactionError::NotFound)?
                    .abort();
                entry.last_activity = Instant::now();
                abort_result?;
                Err(error.into())
            }
        }
    }

    /// Run a query package through the supervisor's normal disposable worker
    /// channels while sharing the transaction retained by the interactive
    /// registry. A successful callback leaves the transaction open; only the
    /// controller's later COMMIT can publish it. Worker/setup errors make the
    /// transaction abort-only and abort the shared storage context.
    pub fn execute_interactive_package_with_supervisor<T, E>(
        &self,
        request: InteractivePackageRequest<'_>,
        execute: impl FnOnce(&mut SupervisorInvocation<'db>) -> Result<T, E>,
    ) -> Result<T, InteractivePackageExecutionError<E>> {
        let InteractivePackageRequest {
            token_id,
            token_secret,
            transaction_id,
            package_id,
            package_digest,
            requested_scope,
            actor,
        } = request;
        if package_id == [0; 16] || transaction_id == [0; 16] {
            return Err(InteractivePackageExecutionError::Transaction(
                InteractiveTransactionError::InvalidRequest,
            ));
        }
        let session = self
            .authenticate_query_session(token_id, token_secret)
            .map_err(|error| InteractivePackageExecutionError::Transaction(error.into()))?;
        let actor_session = match actor {
            Some(proof) => {
                let actor_session = self
                    .authenticate_query_session(proof.token_id, proof.token_secret)
                    .map_err(|error| InteractivePackageExecutionError::Transaction(error.into()))?;
                if actor_session.principal_name() != proof.expected_name {
                    return Err(InteractivePackageExecutionError::Transaction(
                        InteractiveTransactionError::InvalidActor,
                    ));
                }
                actor_session
            }
            None => session.clone(),
        };
        let actor_record = Some(InteractivePackageActorRecord {
            token_id: actor.map_or(token_id, |proof| proof.token_id),
            principal_id: actor_session.principal_id(),
            principal_name: actor_session.principal_name().to_owned(),
            authority_epoch: actor_session.authority_epoch(),
            role_ids: actor_session.role_ids().clone(),
        });
        let actor_id = actor_session.principal_id();
        let actor_role_ids = actor_session.role_ids().clone();
        let entry = self
            .interactive_transaction_entry(transaction_id)
            .map_err(InteractivePackageExecutionError::Transaction)?;
        let mut entry = self
            .try_lock_interactive_transaction(&entry)
            .map_err(InteractivePackageExecutionError::Transaction)?;
        if session.principal_id() != entry.controller_id {
            return Err(InteractivePackageExecutionError::Transaction(
                InteractiveTransactionError::NotOwner,
            ));
        }
        self.expire_interactive_transaction_if_needed(&mut entry)
            .map_err(InteractivePackageExecutionError::Transaction)?;
        match entry.state {
            InteractiveTransactionState::Active => (),
            InteractiveTransactionState::AbortOnly => {
                return Err(InteractivePackageExecutionError::Transaction(
                    InteractiveTransactionError::AbortOnly,
                ));
            }
            InteractiveTransactionState::Expired => {
                return Err(InteractivePackageExecutionError::Transaction(
                    InteractiveTransactionError::Expired,
                ));
            }
            _ => {
                return Err(InteractivePackageExecutionError::Transaction(
                    InteractiveTransactionError::NotFound,
                ));
            }
        }
        if entry.packages.contains_key(&package_id) {
            let same_package = entry
                .packages
                .get(&package_id)
                .is_some_and(|record| record.digest == package_digest);
            return Err(InteractivePackageExecutionError::Transaction(
                if same_package {
                    InteractiveTransactionError::DuplicatePackage
                } else {
                    InteractiveTransactionError::PackageConflict
                },
            ));
        }
        if entry.packages.len() >= INTERACTIVE_TRANSACTION_MAX_PACKAGES {
            return Err(InteractivePackageExecutionError::Transaction(
                InteractiveTransactionError::PackageLimit,
            ));
        }
        entry.packages.insert(
            package_id,
            InteractivePackageRecord {
                digest: package_digest,
                state: InteractivePackageState::Running,
                result_lost: false,
                actor: actor_record.clone(),
            },
        );
        entry.last_activity = Instant::now();
        let transaction_handle = Arc::clone(&entry.transaction);
        {
            let mut owner = transaction_handle.lock().map_err(|_| {
                InteractivePackageExecutionError::Transaction(
                    InteractiveTransactionError::RegistryPoisoned,
                )
            })?;
            let transaction = owner.as_mut().ok_or_else(|| {
                InteractivePackageExecutionError::Transaction(InteractiveTransactionError::NotFound)
            })?;
            transaction.set_package_requested_scope(Some(requested_scope));
            transaction.set_package_principal(Some(actor_id));
        }
        let mut supervisor = SupervisorInvocation {
            invocation_id: transaction_id,
            principal_id: actor_id,
            role_ids: actor_role_ids,
            transaction: transaction_handle,
            channels: Arc::new(Mutex::new(SupervisorChannelState::default())),
            deadline: entry.started_at.checked_add(self.max_invocation_age),
            abort_on_drop: false,
        };
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute(&mut supervisor)));
        if let Ok(mut owner) = supervisor.transaction.lock()
            && let Some(transaction) = owner.as_mut()
        {
            transaction.set_package_requested_scope(None);
            transaction.set_package_principal(None);
        }
        match result {
            Ok(Ok(value)) => {
                let transaction_live = supervisor
                    .transaction
                    .lock()
                    .map(|owner| owner.is_some())
                    .unwrap_or(false);
                if transaction_live {
                    if let Some(record) = entry.packages.get_mut(&package_id) {
                        record.state = InteractivePackageState::Completed;
                    }
                    entry.last_activity = Instant::now();
                    Ok(value)
                } else {
                    if let Some(record) = entry.packages.get_mut(&package_id) {
                        record.state = InteractivePackageState::Failed;
                    }
                    entry.state = InteractiveTransactionState::AbortOnly;
                    Err(InteractivePackageExecutionError::Transaction(
                        InteractiveTransactionError::NotFound,
                    ))
                }
            }
            Ok(Err(error)) => {
                if let Some(record) = entry.packages.get_mut(&package_id) {
                    record.state = InteractivePackageState::Failed;
                }
                entry.state = InteractiveTransactionState::AbortOnly;
                if let Ok(mut owner) = supervisor.transaction.lock()
                    && let Some(mut transaction) = owner.take()
                {
                    let _ = transaction.abort();
                }
                entry.last_activity = Instant::now();
                Err(InteractivePackageExecutionError::Execution(error))
            }
            Err(_) => {
                if let Some(record) = entry.packages.get_mut(&package_id) {
                    record.state = InteractivePackageState::Failed;
                }
                entry.state = InteractiveTransactionState::AbortOnly;
                if let Ok(mut owner) = supervisor.transaction.lock()
                    && let Some(mut transaction) = owner.take()
                {
                    let _ = transaction.abort();
                }
                entry.last_activity = Instant::now();
                Err(InteractivePackageExecutionError::Panicked)
            }
        }
    }

    /// Read package execution state without returning or replaying result
    /// bytes. The actor is currently the transaction controller; RUNAS actor
    /// binding is added by the later authorization slice.
    pub fn interactive_package_status(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
        package_id: [u8; 16],
    ) -> Result<InteractivePackageStatus, InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry = entry
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
        if session.principal_id() != entry.controller_id {
            return Err(InteractiveTransactionError::NotOwner);
        }
        self.expire_interactive_transaction_if_needed(&mut entry)?;
        entry.last_activity = Instant::now();
        Ok(match entry.packages.get(&package_id) {
            Some(record) => InteractivePackageStatus {
                state: record.state,
                result_lost: record.result_lost,
            },
            None => InteractivePackageStatus {
                state: InteractivePackageState::Unknown,
                result_lost: false,
            },
        })
    }

    /// Record that a completed package's result stream could not be delivered.
    /// Status can then reconcile execution without making the bytes replayable.
    pub fn mark_interactive_package_result_lost(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
        package_id: [u8; 16],
    ) -> Result<(), InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry = entry
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
        if session.principal_id() != entry.controller_id {
            return Err(InteractiveTransactionError::NotOwner);
        }
        let record = entry
            .packages
            .get_mut(&package_id)
            .ok_or(InteractiveTransactionError::NotFound)?;
        if record.state != InteractivePackageState::Completed {
            return Err(InteractiveTransactionError::InvalidRequest);
        }
        record.result_lost = true;
        entry.last_activity = Instant::now();
        Ok(())
    }

    /// Return live state or a retained durable decision after authenticating
    /// the current controller credential.
    pub fn interactive_transaction_status(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
    ) -> Result<InteractiveTransactionState, InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        match self.interactive_transaction_entry(transaction_id) {
            Ok(entry) => {
                let mut entry = entry
                    .lock()
                    .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
                if session.principal_id() != entry.controller_id {
                    return Err(InteractiveTransactionError::NotOwner);
                }
                self.expire_interactive_transaction_if_needed(&mut entry)?;
                entry.last_activity = Instant::now();
                return Ok(entry.state);
            }
            Err(InteractiveTransactionError::NotFound) => (),
            Err(error) => return Err(error),
        }
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        let status = self
            .get_invocation_status(transaction_id, session.principal_id(), now_us)
            .map_err(|_| InteractiveTransactionError::Transaction(TransactionError::NotFound))?
            .ok_or(InteractiveTransactionError::NotFound)?;
        match status.status {
            InvocationStatus::Terminal(outcome) => match outcome.status {
                crate::outcome::OutcomeStatus::Committed => {
                    Ok(InteractiveTransactionState::Committed)
                }
                crate::outcome::OutcomeStatus::Aborted => Ok(InteractiveTransactionState::Aborted),
            },
            InvocationStatus::Active { .. } | InvocationStatus::Committing { .. } => {
                Err(InteractiveTransactionError::NotFound)
            }
        }
    }

    /// Reap idle/lifetime-expired transactions and terminal package records in
    /// a bounded pass. The supervisor should call this from its periodic
    /// maintenance loop; a caller may also invoke it before admitting work.
    pub fn reap_interactive_transactions(
        &self,
        now_us: i64,
        max_entries: usize,
    ) -> Result<usize, InteractiveTransactionError> {
        if now_us <= 0 || max_entries == 0 {
            return Err(InteractiveTransactionError::InvalidRequest);
        }
        let candidates = {
            let registry = self
                .interactive_transactions
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
            registry
                .iter()
                .take(max_entries.min(1024))
                .map(|(id, entry)| (*id, Arc::clone(entry)))
                .collect::<Vec<_>>()
        };
        let mut remove = Vec::new();
        let mut reaped = 0usize;
        for (transaction_id, entry) in candidates {
            let mut entry = match entry.try_lock() {
                Ok(entry) => entry,
                Err(std::sync::TryLockError::WouldBlock) => continue,
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(InteractiveTransactionError::RegistryPoisoned);
                }
            };
            let was_active = entry.state == InteractiveTransactionState::Active;
            self.expire_interactive_transaction_if_needed(&mut entry)?;
            if was_active && entry.state == InteractiveTransactionState::Expired {
                reaped = reaped.saturating_add(1);
            }
            if matches!(
                entry.state,
                InteractiveTransactionState::Committed
                    | InteractiveTransactionState::Aborted
                    | InteractiveTransactionState::Expired
            ) && entry.outcome_retention_deadline_us <= now_us
            {
                remove.push(transaction_id);
            }
        }
        if !remove.is_empty() {
            let mut registry = self
                .interactive_transactions
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
            for transaction_id in remove {
                if registry.remove(&transaction_id).is_some() {
                    reaped = reaped.saturating_add(1);
                }
            }
        }
        Ok(reaped)
    }

    /// Atomically publish every package's writes and release the retained
    /// snapshot. Only the authenticated controller can commit.
    pub fn commit_interactive_transaction(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
    ) -> Result<u64, InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let transaction = {
            let mut entry = self.try_lock_interactive_transaction(&entry)?;
            if session.principal_id() != entry.controller_id {
                return Err(InteractiveTransactionError::NotOwner);
            }
            self.expire_interactive_transaction_if_needed(&mut entry)?;
            if entry.state == InteractiveTransactionState::AbortOnly {
                return Err(InteractiveTransactionError::AbortOnly);
            }
            if entry.state != InteractiveTransactionState::Active {
                return Err(InteractiveTransactionError::NotFound);
            }
            let staged_actor_ids = entry
                .transaction
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
                .as_ref()
                .ok_or(InteractiveTransactionError::NotFound)?
                .staged_write_actor_ids()?;
            let actor_authority_current = staged_actor_ids.into_iter().all(|writer_id| {
                entry.packages.values().any(|package| {
                    let Some(actor) = &package.actor else {
                        return false;
                    };
                    actor.principal_id == writer_id
                        && self
                            .resolve_query_identity_by_token_id(actor.token_id)
                            .is_ok_and(|identity| {
                                identity.principal_id == actor.principal_id
                                    && identity.principal_name == actor.principal_name
                                    && identity.authority_epoch == actor.authority_epoch
                                    && identity.role_ids == actor.role_ids
                            })
                })
            });
            if !actor_authority_current {
                entry.state = InteractiveTransactionState::AbortOnly;
                if let Some(mut transaction) = entry
                    .transaction
                    .lock()
                    .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
                    .take()
                {
                    transaction.abort()?;
                }
                entry.last_activity = Instant::now();
                return Err(InteractiveTransactionError::InvalidActor);
            }
            entry.state = InteractiveTransactionState::Committing;
            entry
                .transaction
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
                .take()
                .ok_or(InteractiveTransactionError::NotFound)?
        };
        let outcome = transaction.commit();
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry = entry
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?;
        match &outcome {
            Ok(_) => entry.state = InteractiveTransactionState::Committed,
            Err(_) => {
                let status = now_unix_micros().and_then(|now_us| {
                    self.get_invocation_status(transaction_id, session.principal_id(), now_us)
                        .ok()
                        .flatten()
                });
                entry.state = match status.map(|status| status.status) {
                    Some(InvocationStatus::Terminal(outcome))
                        if outcome.status == crate::outcome::OutcomeStatus::Aborted =>
                    {
                        InteractiveTransactionState::Aborted
                    }
                    _ => InteractiveTransactionState::Committing,
                };
            }
        }
        if outcome.is_ok() {
            remove_interactive_transaction_marker(self.database, transaction_id).map_err(
                |error| InteractiveTransactionError::Transaction(TransactionError::Storage(error)),
            )?;
        }
        outcome.map_err(InteractiveTransactionError::Transaction)
    }

    /// Abort the complete transaction and release its retained snapshot.
    pub fn rollback_interactive_transaction(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        transaction_id: [u8; 16],
    ) -> Result<(), InteractiveTransactionError> {
        let session = self.authenticate_query_session(token_id, token_secret)?;
        let entry = self.interactive_transaction_entry(transaction_id)?;
        let mut entry_guard = self.try_lock_interactive_transaction(&entry)?;
        if session.principal_id() != entry_guard.controller_id {
            return Err(InteractiveTransactionError::NotOwner);
        }
        self.expire_interactive_transaction_if_needed(&mut entry_guard)?;
        if !matches!(
            entry_guard.state,
            InteractiveTransactionState::Active | InteractiveTransactionState::AbortOnly
        ) {
            return Err(InteractiveTransactionError::NotFound);
        }
        if let Some(mut transaction) = entry_guard
            .transaction
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
            .take()
        {
            transaction.abort()?;
        }
        remove_interactive_transaction_marker(self.database, transaction_id)
            .map_err(TransactionError::Storage)?;
        entry_guard.state = InteractiveTransactionState::Aborted;
        entry_guard.last_activity = Instant::now();
        Ok(())
    }

    pub(super) fn interactive_transaction_entry(
        &self,
        transaction_id: [u8; 16],
    ) -> Result<Arc<Mutex<InteractiveTransactionEntry<'db>>>, InteractiveTransactionError> {
        self.interactive_transactions
            .lock()
            .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
            .get(&transaction_id)
            .cloned()
            .ok_or(InteractiveTransactionError::NotFound)
    }

    pub(super) fn try_lock_interactive_transaction<'entry>(
        &self,
        entry: &'entry Arc<Mutex<InteractiveTransactionEntry<'db>>>,
    ) -> Result<
        std::sync::MutexGuard<'entry, InteractiveTransactionEntry<'db>>,
        InteractiveTransactionError,
    > {
        match entry.try_lock() {
            Ok(entry) => Ok(entry),
            Err(std::sync::TryLockError::WouldBlock) => {
                Err(InteractiveTransactionError::ConcurrentOperation)
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(InteractiveTransactionError::RegistryPoisoned)
            }
        }
    }

    pub(super) fn expire_interactive_transaction_if_needed(
        &self,
        entry: &mut InteractiveTransactionEntry<'db>,
    ) -> Result<(), InteractiveTransactionError> {
        if entry.state != InteractiveTransactionState::Active {
            return Ok(());
        }
        if entry.started_at.elapsed() >= self.max_invocation_age
            || entry.last_activity.elapsed() >= INTERACTIVE_TRANSACTION_IDLE_TIMEOUT
        {
            if let Some(mut transaction) = entry
                .transaction
                .lock()
                .map_err(|_| InteractiveTransactionError::RegistryPoisoned)?
                .take()
            {
                transaction.abort()?;
            }
            remove_interactive_transaction_marker(self.database, entry.transaction_id)
                .map_err(TransactionError::Storage)?;
            entry.state = InteractiveTransactionState::Expired;
        }
        Ok(())
    }
}
