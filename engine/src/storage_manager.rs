//! Coordinated entry points for invocation commits and administrative DDL.

use crate::audit::AuditEvent;
use crate::commit::{CatalogWrite, CommitError, commit_catalog_and_audit_with_fences};
use crate::ddl_fence::{DdlFenceCoordinator, DdlFenceManager};
use crate::transaction::{InvocationTransaction, TransactionError};
use redb::Database;
use std::time::Duration;

/// Shares one per-table DDL fence manager across row and catalog operations for
/// a database handle.
#[derive(Clone)]
pub struct StorageManager<'db> {
    database: &'db Database,
    ddl_fences: DdlFenceCoordinator,
}

impl<'db> StorageManager<'db> {
    pub fn new(database: &'db Database, ddl_fence_timeout: Duration) -> Self {
        Self {
            database,
            ddl_fences: DdlFenceCoordinator::new(DdlFenceManager::new(), ddl_fence_timeout),
        }
    }

    pub fn begin_invocation(
        &self,
        invocation_id: [u8; 16],
        principal_id: [u8; 16],
        retention_deadline_us: i64,
        max_rows: usize,
        max_bytes: usize,
    ) -> Result<InvocationTransaction<'db>, TransactionError> {
        InvocationTransaction::begin_with_fences(
            self.database,
            self.ddl_fences.clone(),
            invocation_id,
            principal_id,
            retention_deadline_us,
            max_rows,
            max_bytes,
        )
    }

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

    pub fn database(&self) -> &Database {
        self.database
    }
}
