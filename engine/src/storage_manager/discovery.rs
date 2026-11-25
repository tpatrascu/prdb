use super::admission::*;
use super::session_auth::*;
use super::*;
use crate::credentials::{TOKEN_SECRET_BYTES, now_unix_micros};
use crate::transaction::{InvocationCoordination, InvocationTransaction, TransactionError};

impl<'db> StorageManager<'db> {
    /// Discover every schema visible to a token-authenticated principal from
    /// one request-scoped catalog snapshot. This read does not register an
    /// invocation or write a durable outcome. The bounded result is suitable
    /// for client-side requested-scope construction.
    pub fn discover_token_authorized_schemas(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
    ) -> Result<Vec<crate::metadata_discovery::VisibleSchema>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;

        let mut schemas = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut continuation = None;
        loop {
            let page = transaction.list_catalog_authorized_schemas(
                crate::metadata_discovery::MAX_SCHEMAS_PER_PAGE,
                continuation,
            )?;
            if schemas.len().saturating_add(page.schemas.len()) > MAX_DISCOVERED_SCHEMAS {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            for schema in &page.schemas {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(schema.name.len());
                if encoded_bytes > MAX_DISCOVERED_SCHEMA_BYTES {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            schemas.extend(page.schemas);
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        for schema in transaction.list_catalog_administrative_schemas()? {
            if !schemas
                .iter()
                .any(|existing| existing.schema_id == schema.schema_id)
            {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(schema.name.len());
                if schemas.len() >= MAX_DISCOVERED_SCHEMAS
                    || encoded_bytes > MAX_DISCOVERED_SCHEMA_BYTES
                {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                schemas.push(schema);
            }
        }
        schemas.sort_unstable_by_key(|schema| schema.schema_id);
        Ok(schemas)
    }

    /// Discover relations for one schema visible to a token-authenticated
    /// principal. Hidden and absent schemas return `None`; an authorized
    /// empty schema returns an empty vector. The read uses one request-scoped
    /// catalog snapshot and does not admit an invocation or write an outcome.
    pub fn discover_token_authorized_relations(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::VisibleRelation>>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        if schema_id == crate::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID {
            return Ok(Some(
                crate::metadata_discovery::information_schema_relations().into(),
            ));
        }
        if let Some(relations) = transaction.list_catalog_administrative_relations(schema_id)? {
            return Ok(Some(relations));
        }

        let mut relations = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut continuation = None;
        loop {
            let Some(page) = transaction.list_catalog_authorized_relations(
                schema_id,
                crate::metadata_discovery::MAX_RELATIONS_PER_PAGE,
                continuation,
            )?
            else {
                return Ok(None);
            };
            if relations.len().saturating_add(page.relations.len()) > MAX_DISCOVERED_RELATIONS {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            for relation in &page.relations {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(relation.name.len());
                if encoded_bytes > MAX_DISCOVERED_RELATION_BYTES {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            relations.extend(page.relations);
            continuation = page.continuation;
            if continuation.is_none() {
                return Ok(Some(relations));
            }
        }
    }

    /// Describe every declared column for a relation visible to a
    /// token-authenticated principal. Hidden and absent schema/relation pairs
    /// return `None`; the public information_schema shapes are always visible.
    /// The bounded read uses a single catalog snapshot and creates no
    /// invocation admission or durable outcome.
    pub fn discover_token_authorized_columns(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::VisibleColumn>>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        if schema_id != crate::metadata_discovery::INFORMATION_SCHEMA_SCHEMA_ID
            && let Some(columns) =
                transaction.describe_catalog_administrative_columns(schema_id, relation_id)?
        {
            return Ok(Some(columns));
        }

        let mut columns = Vec::new();
        let mut encoded_bytes = 0usize;
        let mut continuation = None;
        loop {
            let Some(page) = transaction.describe_catalog_authorized_columns_page(
                schema_id,
                relation_id,
                crate::metadata_discovery::MAX_COLUMNS_PER_PAGE,
                continuation,
            )?
            else {
                return Ok(None);
            };
            if columns.len().saturating_add(page.columns.len()) > MAX_DISCOVERED_COLUMNS {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            for column in &page.columns {
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(column.name.len())
                    .saturating_add(
                        crate::metadata_discovery::scalar_type_name(column.scalar_type).len(),
                    )
                    .saturating_add(5);
                if encoded_bytes > MAX_DISCOVERED_COLUMN_BYTES {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
            }
            columns.extend(page.columns);
            continuation = page.continuation;
            if continuation.is_none() {
                return Ok(Some(columns));
            }
        }
    }

    /// List logical index definitions for a relation only when the
    /// authenticated control principal has administrative schema authority.
    /// Hidden and absent relations return `None`.
    pub fn discover_token_administrative_indexes(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeIndex>>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        let Some(indexes) =
            transaction.list_catalog_administrative_indexes(schema_id, relation_id)?
        else {
            return Ok(None);
        };
        let mut encoded_bytes = 0usize;
        for index in &indexes {
            encoded_bytes = encoded_bytes
                .saturating_add(17)
                .saturating_add(index.column_ids.len().saturating_mul(16));
            if encoded_bytes > MAX_DISCOVERED_INDEX_BYTES {
                return Err(TransactionError::InvalidMetadataRequest);
            }
        }
        if indexes.len() > MAX_DISCOVERED_INDEXES {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(indexes))
    }

    /// Resolve one user or role name for an authenticated identity manager.
    /// Unauthorized and absent names share the same `None` result.
    pub fn resolve_token_administrative_identity_name(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        transaction.resolve_catalog_administrative_identity_name(name, kind)
    }

    /// Resolve a policy identity name only for an authenticated owner or
    /// MANAGE_POLICIES holder on the policy's schema/table, or a database
    /// administrator. Unauthorized and absent identities share `None`.
    pub fn resolve_token_policy_identity_name(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        table_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        transaction.resolve_catalog_policy_identity_name(schema_id, table_id, name, kind)
    }

    /// Resolve a USER or ROLE name only for an authenticated schema/table
    /// ALTER grantee, owner, or database administrator. Owner transfer
    /// rechecks current authority at mutation time.
    pub fn resolve_token_alter_identity_name(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        object_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        transaction.resolve_catalog_alter_identity_name(object_id, name, kind)
    }

    /// Resolve a grant recipient within the authenticated grant authority of
    /// one schema or table. Unauthorized and absent identities both return None.
    pub fn resolve_token_grant_target_identity_name(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_name: &str,
        table_name: Option<&str>,
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<
        Option<crate::metadata_discovery::AdministrativeGrantIdentityResolution>,
        TransactionError,
    > {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        transaction.resolve_catalog_grant_target_identity_name(schema_name, table_name, name, kind)
    }

    /// Resolve a policy name inside an authenticated schema/table scope.
    /// Absent names and callers without policy DDL authority share `None`.
    pub fn resolve_token_policy_name(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        table_id: [u8; 16],
        name: &str,
        is_cell: Option<bool>,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        transaction.resolve_catalog_policy_name(schema_id, table_id, name, is_cell)
    }

    /// Describe logical constraints for a relation only when the
    /// authenticated control principal has administrative schema authority.
    /// Hidden foreign-key targets are redacted from the logical response.
    pub fn discover_token_administrative_constraints(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeConstraint>>, TransactionError>
    {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        let Some(constraints) =
            transaction.list_catalog_administrative_constraints(schema_id, relation_id)?
        else {
            return Ok(None);
        };
        let mut encoded_bytes = 0usize;
        for constraint in &constraints {
            encoded_bytes = encoded_bytes
                .saturating_add(19)
                .saturating_add(constraint.kind.as_wire_name().len())
                .saturating_add(constraint.column_ids.len().saturating_mul(16))
                .saturating_add(usize::from(constraint.referenced_table_id.is_some()) * 16)
                .saturating_add(constraint.referenced_column_ids.len().saturating_mul(16));
            if encoded_bytes > MAX_DISCOVERED_CONSTRAINT_BYTES {
                return Err(TransactionError::InvalidMetadataRequest);
            }
        }
        if constraints.len() > MAX_DISCOVERED_CONSTRAINTS {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(constraints))
    }

    /// List direct table/column grants when the authenticated principal has
    /// current administrative schema authority.
    pub fn discover_token_administrative_grants(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeGrant>>, TransactionError> {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        let Some(grants) =
            transaction.list_catalog_administrative_grants(schema_id, relation_id)?
        else {
            return Ok(None);
        };
        let encoded_bytes = grants.iter().fold(0usize, |total, grant| {
            total
                .saturating_add(81)
                .saturating_add(grant.privilege.as_wire_name().len())
                .saturating_add(grant.target_scope.as_wire_name().len())
        });
        if grants.len() > MAX_DISCOVERED_GRANTS || encoded_bytes > MAX_DISCOVERED_GRANT_BYTES {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(grants))
    }

    /// List secret-free policy summaries when the authenticated principal has
    /// current schema-owner/database-administrator authority.
    pub fn discover_token_administrative_policies(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativePolicy>>, TransactionError>
    {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        let Some(policies) =
            transaction.list_catalog_administrative_policies(schema_id, relation_id)?
        else {
            return Ok(None);
        };
        let encoded_bytes = policies.iter().fold(0usize, |total, policy| {
            total
                .saturating_add(33)
                .saturating_add(policy.column_ids.len().saturating_mul(16))
                .saturating_add(
                    policy
                        .recipients
                        .as_ref()
                        .map_or(1, |recipients| recipients.len().saturating_mul(21)),
                )
        });
        if policies.len() > MAX_DISCOVERED_POLICIES || encoded_bytes > MAX_DISCOVERED_POLICY_BYTES {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(policies))
    }

    /// List direct dependency edges touching an administratively visible
    /// object, omitting any edge with a hidden endpoint.
    pub fn discover_token_administrative_dependencies(
        &self,
        token_id: [u8; 16],
        token_secret: &[u8; TOKEN_SECRET_BYTES],
        object_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeDependency>>, TransactionError>
    {
        let principal_id = lookup_credential_principal(self.database, token_id)?;
        let mut invocation_id = [0; 16];
        getrandom::fill(&mut invocation_id)
            .map_err(|_| TransactionError::Storage("randomness unavailable".into()))?;
        if invocation_id == [0; 16] {
            return Err(TransactionError::Storage("randomness unavailable".into()));
        }
        let mut transaction = InvocationTransaction::begin_inner(
            self.database,
            InvocationCoordination::uncoordinated(),
            invocation_id,
            principal_id,
            i64::MAX,
            1,
            1,
        )?;
        let now_us = now_unix_micros().ok_or(TransactionError::ClockUnavailable)?;
        transaction.authenticate_credential_token(token_id, token_secret, now_us)?;
        let Some(dependencies) = transaction.list_catalog_administrative_dependencies(object_id)?
        else {
            return Ok(None);
        };
        let encoded_bytes = dependencies.iter().fold(0usize, |total, edge| {
            total.saturating_add(35 + edge.kind.as_wire_name().len())
        });
        if dependencies.len() > MAX_DISCOVERED_DEPENDENCIES
            || encoded_bytes > MAX_DISCOVERED_DEPENDENCY_BYTES
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(dependencies))
    }
}
