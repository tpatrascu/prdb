//! Coordinated entry points for invocation commits and administrative DDL.

use crate::audit::{AuditAction, AuditEvent, AuditResult};
use crate::catalog::decode_registered_envelope;
use crate::commit::{
    CatalogWrite, CommitError, commit_authorized_constraint_ddl_with_fences,
    commit_authorized_foreign_key_ddl_with_fences, commit_authorized_identity_ddl_with_fences,
    commit_authorized_index_ddl_with_fences, commit_authorized_membership_ddl_with_fences,
    commit_authorized_object_grant_ddl_with_fences, commit_authorized_policy_ddl_with_fences,
    commit_authorized_system_privilege_ddl_with_fences, commit_catalog_and_audit_with_fences,
};
use crate::credentials::{
    MAX_TOKEN_LABEL_BYTES, TOKEN_SECRET_BYTES, decode_verifier_record, encode_new_verifier_record,
    encode_revoked_verifier_record, now_unix_micros,
};
use crate::ddl_fence::{DdlFenceCoordinator, DdlFenceManager};
use crate::outcome::{InvocationOutcome, lookup_outcome};
use crate::schema::CATALOG;
use crate::startup::{StartupError, validate_persisted_records};
use crate::transaction::{InvocationCoordination, InvocationTransaction, TransactionError};
use redb::{Database, ReadableDatabase};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use zeroize::Zeroize;

/// Admission and coordination limits for one storage-manager handle.
///
/// Defaults allow at most 128 simultaneous invocation snapshots and a
/// 15-minute lifetime for each managed invocation.
#[derive(Clone, Copy, Debug)]
pub struct StorageManagerConfig {
    pub ddl_fence_timeout: Duration,
    pub max_active_invocations: usize,
    pub max_invocation_age: Duration,
}

impl Default for StorageManagerConfig {
    fn default() -> Self {
        Self {
            ddl_fence_timeout: Duration::from_secs(5),
            max_active_invocations: 128,
            max_invocation_age: Duration::from_secs(15 * 60),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageManagerConfigError {
    ZeroActiveInvocationLimit,
    ZeroInvocationAge,
}

impl std::fmt::Display for StorageManagerConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "invalid storage manager configuration: {self:?}")
    }
}

impl std::error::Error for StorageManagerConfigError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StorageManagerInitError {
    Config(StorageManagerConfigError),
    Startup(StartupError),
}

impl std::fmt::Display for StorageManagerInitError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "storage manager initialization failed: {self:?}")
    }
}

impl std::error::Error for StorageManagerInitError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TokenLifecycleError {
    InvalidRequest,
    InvalidAudit,
    RandomnessUnavailable,
    TokenIdGenerationFailed,
    TokenUnavailable,
    EncodingFailed,
    Storage(String),
    Commit(CommitError),
}

impl std::fmt::Display for TokenLifecycleError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "token lifecycle operation failed: {self:?}")
    }
}

impl std::error::Error for TokenLifecycleError {}

#[derive(Clone, Debug)]
pub struct TokenIssueRequest<'a> {
    pub invocation_id: [u8; 16],
    pub authenticated_actor_id: [u8; 16],
    pub user_id: [u8; 16],
    pub label: &'a str,
    pub expires_at_us: i64,
    pub retention_deadline_us: i64,
    pub audit_event: AuditEvent,
}

/// Newly issued bearer token. Call [`Self::take_secret`] once to transfer the
/// secret to the protected caller; any secret still held here is zeroized on
/// drop. This type intentionally has no `Debug` implementation.
pub struct IssuedToken {
    pub token_id: [u8; 16],
    pub commit_sequence: u64,
    secret: Option<[u8; TOKEN_SECRET_BYTES]>,
}

impl IssuedToken {
    pub fn take_secret(&mut self) -> Option<[u8; TOKEN_SECRET_BYTES]> {
        self.secret.take()
    }
}

impl Drop for IssuedToken {
    fn drop(&mut self) {
        if let Some(secret) = &mut self.secret {
            secret.zeroize();
        }
    }
}

/// Shares one per-table DDL fence manager across row and catalog operations for
/// a database handle.
#[derive(Clone)]
pub struct StorageManager<'db> {
    database: &'db Database,
    ddl_fences: DdlFenceCoordinator,
    max_active_invocations: usize,
    admissions: Arc<InvocationAdmissionState>,
    max_invocation_age: Duration,
}

struct InvocationAdmissionState {
    active: AtomicUsize,
    limit: usize,
}

pub(crate) struct InvocationAdmissionPermit {
    state: Arc<InvocationAdmissionState>,
}

impl InvocationAdmissionState {
    fn acquire(self: &Arc<Self>) -> Option<InvocationAdmissionPermit> {
        let mut active = self.active.load(Ordering::Acquire);
        loop {
            if active >= self.limit {
                return None;
            }
            match self.active.compare_exchange_weak(
                active,
                active + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(InvocationAdmissionPermit {
                        state: Arc::clone(self),
                    });
                }
                Err(actual) => active = actual,
            }
        }
    }
}

impl Drop for InvocationAdmissionPermit {
    fn drop(&mut self) {
        self.state.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<'db> StorageManager<'db> {
    /// Validate the recovered database, then use the default snapshot limit
    /// and the supplied DDL-fence timeout.
    pub fn new(
        database: &'db Database,
        ddl_fence_timeout: Duration,
    ) -> Result<Self, StorageManagerInitError> {
        Self::with_config(
            database,
            StorageManagerConfig {
                ddl_fence_timeout,
                ..StorageManagerConfig::default()
            },
        )
    }

    pub fn with_config(
        database: &'db Database,
        config: StorageManagerConfig,
    ) -> Result<Self, StorageManagerInitError> {
        if config.max_active_invocations == 0 {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroActiveInvocationLimit,
            ));
        }
        if config.max_invocation_age.is_zero() {
            return Err(StorageManagerInitError::Config(
                StorageManagerConfigError::ZeroInvocationAge,
            ));
        }
        validate_persisted_records(database).map_err(StorageManagerInitError::Startup)?;
        Ok(Self {
            database,
            ddl_fences: DdlFenceCoordinator::new(DdlFenceManager::new(), config.ddl_fence_timeout),
            max_active_invocations: config.max_active_invocations,
            admissions: Arc::new(InvocationAdmissionState {
                active: AtomicUsize::new(0),
                limit: config.max_active_invocations,
            }),
            max_invocation_age: config.max_invocation_age,
        })
    }

    /// The configured hard cap shared by clones of this manager.
    pub fn max_active_invocations(&self) -> usize {
        self.max_active_invocations
    }

    pub fn max_invocation_age(&self) -> Duration {
        self.max_invocation_age
    }

    /// Begin a managed invocation after a trusted caller has established its
    /// identity and performed the required admission checks. Service callers
    /// should use [`Self::begin_authenticated_invocation`] to validate the
    /// principal against the pinned catalog. The slot remains held until
    /// commit, abort, expiry, or transaction drop.
    pub fn begin_invocation(
        &self,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<InvocationTransaction<'db>, TransactionError> {
        let admission_permit = self
            .admissions
            .acquire()
            .ok_or(TransactionError::AdmissionLimitExceeded)?;
        InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::managed(
                self.ddl_fences.clone(),
                admission_permit,
                self.max_invocation_age,
            ),
            invocation_id,
            principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )
    }

    /// Admit an invocation for a principal already authenticated by a trusted
    /// caller, then load its role closure from the invocation's pinned catalog
    /// snapshot. This validates that the principal is a live LOGIN user and
    /// guards the identity and membership records through commit. It does not
    /// verify credentials, token expiry, or revocation; callers must complete
    /// those checks before invoking this method.
    pub fn begin_authenticated_invocation(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<(InvocationTransaction<'db>, HashSet<[u8; 16]>), TransactionError> {
        let mut transaction = self.begin_invocation(
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )?;
        let roles = transaction.load_catalog_role_closure(authenticated_principal_id)?;
        Ok((transaction, roles))
    }

    /// Authenticate a database-issued bearer token, then admit the invocation
    /// against one pinned catalog snapshot. Token, user, and membership bytes
    /// are guarded through commit; a concurrent revocation makes the pending
    /// invocation conflict.
    pub fn begin_token_authenticated_invocation(
        &self,
        invocation_id: [u8; 16],
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<(InvocationTransaction<'db>, HashSet<[u8; 16]>), TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut transaction = self.begin_invocation(
            invocation_id,
            principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )?;
        let Some(now_us) = now_unix_micros() else {
            transaction.abort()?;
            return Err(TransactionError::AuthenticationFailed);
        };
        let roles = transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        Ok((transaction, roles))
    }

    /// Issue one random bearer token for a LOGIN user through the authorized
    /// identity DDL path. The secret is returned once and only its verifier is
    /// written to the catalog and audit event.
    pub fn issue_token(
        &self,
        request: TokenIssueRequest<'_>,
    ) -> Result<IssuedToken, TokenLifecycleError> {
        let TokenIssueRequest {
            invocation_id,
            authenticated_actor_id,
            user_id,
            label,
            expires_at_us,
            retention_deadline_us,
            mut audit_event,
        } = request;
        let Some(issued_at_us) = now_unix_micros() else {
            return Err(TokenLifecycleError::InvalidRequest);
        };
        if user_id == [0; 16]
            || authenticated_actor_id == [0; 16]
            || expires_at_us <= issued_at_us
            || label.len() > MAX_TOKEN_LABEL_BYTES
        {
            return Err(TokenLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.action != AuditAction::TokenLifecycle
            || audit_event.result != AuditResult::Succeeded
            || !audit_event.target_ids.contains(&user_id)
        {
            return Err(TokenLifecycleError::InvalidAudit);
        }

        let mut generated = None;
        for _ in 0..8 {
            let mut material = [0u8; 16 + TOKEN_SECRET_BYTES];
            if getrandom::fill(&mut material).is_err() {
                material.zeroize();
                return Err(TokenLifecycleError::RandomnessUnavailable);
            }
            let token_id: [u8; 16] = material[..16].try_into().expect("fixed token ID length");
            let mut secret: [u8; TOKEN_SECRET_BYTES] = material[16..]
                .try_into()
                .expect("fixed token secret length");
            material.zeroize();
            if token_id == [0; 16] {
                secret.zeroize();
                continue;
            }
            match credential_token_exists(self.database, token_id) {
                Ok(false) => {
                    generated = Some((token_id, secret));
                    secret.zeroize();
                    break;
                }
                Ok(true) => secret.zeroize(),
                Err(error) => {
                    secret.zeroize();
                    return Err(error);
                }
            }
        }
        let Some((token_id, mut secret)) = generated else {
            return Err(TokenLifecycleError::TokenIdGenerationFailed);
        };
        let payload = match encode_new_verifier_record(
            token_id,
            user_id,
            &secret,
            issued_at_us,
            expires_at_us,
            label,
        ) {
            Ok(payload) => payload,
            Err(_) => {
                secret.zeroize();
                return Err(TokenLifecycleError::EncodingFailed);
            }
        };
        let mut target_ids = audit_event.target_ids;
        if target_ids.len() >= 256 {
            secret.zeroize();
            return Err(TokenLifecycleError::InvalidAudit);
        }
        target_ids.push(token_id);
        target_ids.sort_unstable();
        target_ids.dedup();
        audit_event.target_ids = target_ids;
        let write = CatalogWrite {
            key: credential_catalog_key(token_id),
            expected_value: None,
            payload: Some(payload),
        };
        let sequence = match self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[write],
            &[audit_event],
        ) {
            Ok(sequence) => sequence,
            Err(error) => {
                secret.zeroize();
                return Err(TokenLifecycleError::Commit(error));
            }
        };
        let issued = IssuedToken {
            token_id,
            commit_sequence: sequence,
            secret: Some(secret),
        };
        secret.zeroize();
        Ok(issued)
    }

    /// Revoke one token through the authorized identity DDL path. Unknown,
    /// already revoked, and unauthorized token requests share
    /// `TokenUnavailable` so this method cannot be used to enumerate tokens.
    pub fn revoke_token(
        &self,
        invocation_id: [u8; 16],
        authenticated_actor_id: [u8; 16],
        token_id: [u8; 16],
        retention_deadline_us: i64,
        mut audit_event: AuditEvent,
    ) -> Result<u64, TokenLifecycleError> {
        if authenticated_actor_id == [0; 16] || token_id == [0; 16] {
            return Err(TokenLifecycleError::InvalidRequest);
        }
        if audit_event.actor_id != Some(authenticated_actor_id)
            || audit_event.action != AuditAction::TokenLifecycle
            || audit_event.result != AuditResult::Succeeded
            || !audit_event.target_ids.contains(&token_id)
        {
            return Err(TokenLifecycleError::InvalidAudit);
        }
        let key = credential_catalog_key(token_id);
        let read = self
            .database
            .begin_read()
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let table = read
            .open_table(CATALOG)
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
        let current_bytes = table
            .get(key.as_slice())
            .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?
            .map(|value| value.value().to_vec())
            .ok_or(TokenLifecycleError::TokenUnavailable)?;
        let current_record = decode_registered_envelope(&current_bytes)
            .map_err(|_| TokenLifecycleError::TokenUnavailable)?;
        if current_record.logical_key != key {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let current = decode_verifier_record(&current_record)
            .map_err(|_| TokenLifecycleError::TokenUnavailable)?;
        if current.revoked {
            return Err(TokenLifecycleError::TokenUnavailable);
        }
        let Some(revoked_at_us) = now_unix_micros() else {
            return Err(TokenLifecycleError::InvalidRequest);
        };
        let payload = encode_revoked_verifier_record(&current_record, revoked_at_us)
            .map_err(|_| TokenLifecycleError::EncodingFailed)?;
        drop(table);
        drop(read);
        let mut target_ids = audit_event.target_ids;
        if target_ids.len() >= 256 {
            return Err(TokenLifecycleError::InvalidAudit);
        }
        target_ids.push(current.user_id);
        target_ids.sort_unstable();
        target_ids.dedup();
        audit_event.target_ids = target_ids;
        let write = CatalogWrite {
            key,
            expected_value: Some(current_bytes),
            payload: Some(payload),
        };
        self.commit_authorized_identity_ddl(
            invocation_id,
            authenticated_actor_id,
            retention_deadline_us,
            &[write],
            &[audit_event],
        )
        .map_err(|error| match error {
            CommitError::DdlAuthorizationDenied => TokenLifecycleError::TokenUnavailable,
            error => TokenLifecycleError::Commit(error),
        })
    }

    /// Trusted coordinated catalog publication with no actor authorization.
    /// Use only from an internal caller that has already applied the relevant
    /// action-specific administrative checks.
    pub fn commit_catalog_and_audit(
        &self,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_catalog_and_audit_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize ordinary index DDL for a principal authenticated by the
    /// trusted administrative channel, then commit it under the shared DDL
    /// fences. Supports family-9 index definitions and matching family-2 index
    /// objects. Other catalog DDL remains on the trusted internal entry point
    /// until its action-specific authority rules are implemented.
    pub fn commit_authorized_index_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_index_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize user/role creation, rename, and login-state changes for an
    /// actor authenticated by the trusted administrative channel. User
    /// lifecycle changes require `MANAGE_USERS`, role changes require
    /// `MANAGE_ROLES`, and identity deletion remains unsupported.
    pub fn commit_authorized_identity_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_identity_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize and commit row/cell policy DDL for an actor authenticated by
    /// the trusted administrative channel. The actor must own each affected
    /// table or hold inherited `MANAGE_POLICIES` on it.
    pub fn commit_authorized_policy_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_policy_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize and commit foreign-key additions/removals on existing
    /// tables. Creation requires source-table `ALTER` and target-table or
    /// target-column `REFERENCES`; removal requires source-table `ALTER`.
    pub fn commit_authorized_foreign_key_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_foreign_key_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize NOT NULL, PRIMARY KEY, UNIQUE, and CHECK constraint add/drop
    /// for an existing table. Key constraints atomically manage their owned
    /// unique backing indexes; additions validate stored rows before
    /// publication. The actor must own the table or hold inherited ALTER on
    /// the table/schema.
    pub fn commit_authorized_constraint_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_constraint_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize and commit role membership changes for an actor authenticated
    /// by the trusted administrative channel. Requires `MANAGE_ROLES`, except
    /// changes to the reserved database-administrator role, which require an
    /// existing administrator.
    pub fn commit_authorized_membership_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_membership_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize system privilege grant/revoke operations for an actor
    /// authenticated by the trusted administrative channel. Only current
    /// database administrators may use this route.
    pub fn commit_authorized_system_privilege_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_system_privilege_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Authorize and commit object grant/revoke changes for an actor
    /// authenticated by the trusted administrative channel. The actor must
    /// own each target, hold `MANAGE_GRANTS` on it, or be a database admin.
    pub fn commit_authorized_object_grant_ddl(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        retention_deadline_us: i64,
        catalog_writes: &[CatalogWrite],
        audit_events: &[AuditEvent],
    ) -> Result<u64, CommitError> {
        commit_authorized_object_grant_ddl_with_fences(
            self.database,
            &self.ddl_fences,
            invocation_id,
            authenticated_principal_id,
            retention_deadline_us,
            catalog_writes,
            audit_events,
        )
    }

    /// Resolve a retained terminal outcome for an already authenticated owner.
    /// Unknown, mismatched, and expired records all return `None`.
    pub fn lookup_invocation_outcome(
        &self,
        invocation_id: [u8; 16],
        authenticated_principal_id: [u8; 16],
        now_us: i64,
    ) -> Result<Option<InvocationOutcome>, Box<dyn std::error::Error>> {
        lookup_outcome(
            self.database,
            invocation_id,
            authenticated_principal_id,
            now_us,
        )
    }
}

fn credential_catalog_key(token_id: [u8; 16]) -> Vec<u8> {
    let mut key = 16u16.to_be_bytes().to_vec();
    key.push(1);
    key.extend_from_slice(&token_id);
    key
}

fn credential_token_exists(
    database: &Database,
    token_id: [u8; 16],
) -> Result<bool, TokenLifecycleError> {
    let read = database
        .begin_read()
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    let table = read
        .open_table(CATALOG)
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))?;
    table
        .get(credential_catalog_key(token_id).as_slice())
        .map(|value| value.is_some())
        .map_err(|error| TokenLifecycleError::Storage(error.to_string()))
}

fn lookup_credential_principal(
    database: &Database,
    token_id: [u8; 16],
) -> Result<[u8; 16], TransactionError> {
    if token_id == [0; 16] {
        return Err(TransactionError::AuthenticationFailed);
    }
    let read = database
        .begin_read()
        .map_err(|error| TransactionError::Storage(error.to_string()))?;
    let table = read
        .open_table(CATALOG)
        .map_err(|error| TransactionError::Storage(error.to_string()))?;
    let key = credential_catalog_key(token_id);
    let bytes = table
        .get(key.as_slice())
        .map_err(|error| TransactionError::Storage(error.to_string()))?
        .map(|value| value.value().to_vec())
        .ok_or(TransactionError::AuthenticationFailed)?;
    let record =
        decode_registered_envelope(&bytes).map_err(|_| TransactionError::AuthenticationFailed)?;
    if record.logical_key != key {
        return Err(TransactionError::AuthenticationFailed);
    }
    let credential =
        decode_verifier_record(&record).map_err(|_| TransactionError::AuthenticationFailed)?;
    Ok(credential.user_id)
}
