use super::catalog_loader::*;
use super::*;
use crate::authorization::{AccessTarget, Operation};
use crate::expression::ExpressionPurpose;
use crate::metadata::EngineMetadata;
use crate::metadata_discovery::{VisibleColumn, VisibleRelation, VisibleSchema};
use crate::schema::PRINCIPAL_NAMES;
use crate::snapshot::SnapshotError;
use std::collections::HashSet;

impl<'db> InvocationTransaction<'db> {
    /// Resolve one normalized principal name only for a caller with the
    /// corresponding identity-management privilege or database-admin role.
    /// The lookup is non-enumerating and remains within this pinned snapshot.
    pub(crate) fn resolve_catalog_administrative_identity_name(
        &mut self,
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let bytes = name.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 128
            || !(bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let is_database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        let metadata_bytes = self
            .pinned_snapshot()?
            .get_engine_metadata_value()
            .map_err(|error| TransactionError::Storage(error.to_string()))?
            .ok_or(TransactionError::NotReady)?;
        let metadata = EngineMetadata::decode(&metadata_bytes)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        let manages_identity_kind = |this: &mut Self, privilege_code: u16| {
            if is_database_admin {
                return Ok(true);
            }
            for subject in &subjects {
                let mut key = 12u16.to_be_bytes().to_vec();
                key.extend_from_slice(&[1]);
                key.extend_from_slice(subject);
                key.extend_from_slice(&[1]);
                key.extend_from_slice(&metadata.database_id);
                key.extend_from_slice(&[1]);
                key.extend_from_slice(&privilege_code.to_be_bytes());
                key.extend_from_slice(&[1]);
                key.extend_from_slice(&1u16.to_be_bytes());
                key.extend_from_slice(&[1]);
                key.extend_from_slice(&metadata.database_id);
                if let Some(record) = this.read_catalog_record(&key)? {
                    if record.family_code != 12
                        || catalog_id(&record, 1)? == [0; 16]
                        || catalog_u64(&record, 2)? == 0
                    {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    return Ok(true);
                }
            }
            Ok(false)
        };
        let manages_users = manages_identity_kind(self, 12)?;
        let manages_roles = manages_identity_kind(self, 13)?;
        let can_lookup_any = match kind {
            crate::metadata_discovery::AdministrativeIdentityKind::User => manages_users,
            crate::metadata_discovery::AdministrativeIdentityKind::Role => manages_roles,
            crate::metadata_discovery::AdministrativeIdentityKind::Either => {
                manages_users || manages_roles
            }
        };
        if !can_lookup_any {
            return Ok(None);
        }
        let snapshot = self.pinned_snapshot()?;
        let name_index = snapshot
            .read_transaction()
            .open_table(PRINCIPAL_NAMES)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        let Some(identity_id) = name_index
            .get(bytes)
            .map_err(|error| TransactionError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let identity_id: [u8; 16] = identity_id
            .value()
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?;
        if identity_id == [0; 16] {
            return Err(TransactionError::CorruptCatalog);
        }
        let principal = self
            .read_catalog_record(&catalog_id_key(10, identity_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if principal.family_code != 10 {
            return Err(TransactionError::CorruptCatalog);
        }
        let principal_kind = catalog_u16(&principal, 1)?;
        if catalog_field(&principal, 2)? != bytes {
            return Err(TransactionError::CorruptCatalog);
        }
        let can_inspect_kind = match (kind, principal_kind) {
            (crate::metadata_discovery::AdministrativeIdentityKind::User, 1) => manages_users,
            (crate::metadata_discovery::AdministrativeIdentityKind::Role, 2) => manages_roles,
            (crate::metadata_discovery::AdministrativeIdentityKind::Either, 1) => manages_users,
            (crate::metadata_discovery::AdministrativeIdentityKind::Either, 2) => manages_roles,
            (_, 1 | 2) => false,
            _ => return Err(TransactionError::CorruptCatalog),
        };
        if !can_inspect_kind {
            return Ok(None);
        }
        Ok(Some(identity_id))
    }

    /// Resolve a policy recipient only when the authenticated principal can
    /// administer policies on this table. This keeps name lookup scoped to
    /// the object whose policy is being created.
    pub(crate) fn resolve_catalog_policy_identity_name(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        let bytes = name.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 128
            || !(bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
            || !bytes
                .iter()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'_')
            || schema_id == [0; 16]
            || table_id == [0; 16]
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        let is_database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        let schema_key = catalog_id_key(2, schema_id);
        let table_key = catalog_id_key(5, table_id);
        let Some(schema) = self.read_catalog_record(&schema_key)? else {
            return Ok(None);
        };
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if schema.family_code != 2
            || catalog_u16(&schema, 1)? != 1
            || table.family_code != 5
            || catalog_id(&table, 1)? != schema_id
        {
            return Ok(None);
        }
        let schema_object = self
            .read_catalog_record(&catalog_id_key(2, schema_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        let table_object = self
            .read_catalog_record(&catalog_id_key(2, table_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if schema_object.family_code != 2
            || catalog_u16(&schema_object, 1)? != 1
            || table_object.family_code != 2
            || catalog_u16(&table_object, 1)? != 3
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let mut has_policy_authority = is_database_admin
            || subjects.contains(&catalog_id(&schema_object, 4)?)
            || subjects.contains(&catalog_id(&table_object, 4)?);
        for object_id in [schema_id, table_id] {
            if has_policy_authority {
                break;
            }
            let grants = self
                .pinned_snapshot()?
                .lookup_grants_for_object(object_id)
                .map_err(|error| match error {
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(grants.len())?;
            for grant in grants {
                let key = &grant.logical_key;
                if grant.family_code != 12
                    || key.len() != 59
                    || key[0..2] != 12u16.to_be_bytes()
                    || key[2] != 1
                    || key[19] != 1
                    || key[20..36] != object_id
                    || key[36] != 1
                    || key[37..39] != 10u16.to_be_bytes()
                    || key[39] != 1
                    || key[40..42] != 1u16.to_be_bytes()
                    || key[42] != 1
                    || key[43..59] != object_id
                {
                    continue;
                }
                if catalog_u64(&grant, 2)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantee: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if grantee == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if subjects.contains(&grantee) {
                    has_policy_authority = true;
                    break;
                }
            }
        }
        if !has_policy_authority {
            return Ok(None);
        }
        let snapshot = self.pinned_snapshot()?;
        let name_index = snapshot
            .read_transaction()
            .open_table(PRINCIPAL_NAMES)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        let Some(identity_id) = name_index
            .get(bytes)
            .map_err(|error| TransactionError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let identity_id: [u8; 16] = identity_id
            .value()
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?;
        if identity_id == [0; 16] {
            return Err(TransactionError::CorruptCatalog);
        }
        let principal = self
            .read_catalog_record(&catalog_id_key(10, identity_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if principal.family_code != 10 {
            return Err(TransactionError::CorruptCatalog);
        }
        let expected_kind = match kind {
            crate::metadata_discovery::AdministrativeIdentityKind::User => 1,
            crate::metadata_discovery::AdministrativeIdentityKind::Role => 2,
            crate::metadata_discovery::AdministrativeIdentityKind::Either => {
                return Err(TransactionError::InvalidMetadataRequest);
            }
        };
        if catalog_u16(&principal, 1)? != expected_kind || catalog_field(&principal, 2)? != bytes {
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(Some(identity_id))
    }

    /// Resolve a grant target and recipient only for an actor authorized to
    /// administer grants on that schema or table. Mutation rechecks authority.
    pub(crate) fn resolve_catalog_grant_target_identity_name(
        &mut self,
        schema_name: &str,
        table_name: Option<&str>,
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<
        Option<crate::metadata_discovery::AdministrativeGrantIdentityResolution>,
        TransactionError,
    > {
        let valid_name = |value: &str| {
            !value.is_empty()
                && value.len() <= 128
                && (value.as_bytes()[0].is_ascii_lowercase() || value.as_bytes()[0] == b'_')
                && value
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        };
        if !valid_name(schema_name) || table_name.is_some_and(|name| !valid_name(name)) {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let indexed_schema_id = {
            self.pinned_snapshot()?
                .get_schema_name_index_value(schema_name.as_bytes())
                .map_err(|error| TransactionError::Storage(error.to_string()))?
        };
        let Some(schema_id) = indexed_schema_id else {
            return Ok(None);
        };
        let schema_id: [u8; 16] = schema_id
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?;
        if schema_id == [0; 16] {
            return Err(TransactionError::CorruptCatalog);
        }
        let schema_object = self
            .read_catalog_record(&catalog_id_key(2, schema_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if schema_object.family_code != 2
            || catalog_u16(&schema_object, 1)? != 1
            || catalog_field(&schema_object, 3)? != schema_name.as_bytes()
        {
            return Err(TransactionError::CorruptCatalog);
        }
        let object_id = if let Some(table_name) = table_name {
            let indexed_table_id = {
                self.pinned_snapshot()?
                    .get_relation_name_index_value(schema_id, table_name.as_bytes())
                    .map_err(|error| TransactionError::Storage(error.to_string()))?
            };
            let Some(table_id) = indexed_table_id else {
                return Ok(None);
            };
            let table_id: [u8; 16] = table_id
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            if table_id == [0; 16] {
                return Err(TransactionError::CorruptCatalog);
            }
            let table_object = self
                .read_catalog_record(&catalog_id_key(2, table_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if table_object.family_code != 2
                || catalog_u16(&table_object, 1)? != 3
                || catalog_optional_id(&table_object, 2)? != Some(schema_id)
                || catalog_field(&table_object, 3)? != table_name.as_bytes()
            {
                return Err(TransactionError::CorruptCatalog);
            }
            table_id
        } else {
            schema_id
        };
        Ok(self
            .resolve_catalog_grant_identity_name(object_id, name, kind)?
            .map(
                |identity_id| crate::metadata_discovery::AdministrativeGrantIdentityResolution {
                    schema_id,
                    object_id,
                    identity_id,
                },
            ))
    }

    /// Resolve a grant recipient only for an actor authorized to administer
    /// grants on the target schema or table. Mutation rechecks authority.
    pub(crate) fn resolve_catalog_grant_identity_name(
        &mut self,
        object_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        self.resolve_catalog_object_ddl_identity_name(object_id, name, kind, 11)
    }

    /// Resolve an owner name only for an actor with ALTER authority over the
    /// target schema or table. The owner-transfer operation rechecks authority.
    pub(crate) fn resolve_catalog_alter_identity_name(
        &mut self,
        object_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        self.resolve_catalog_object_ddl_identity_name(object_id, name, kind, 8)
    }

    pub(super) fn resolve_catalog_object_ddl_identity_name(
        &mut self,
        object_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
        privilege_code: u16,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        if name.is_empty()
            || name.len() > 128
            || !(name.as_bytes()[0].is_ascii_lowercase() || name.as_bytes()[0] == b'_')
            || !name
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            || object_id == [0; 16]
            || !matches!(privilege_code, 8 | 9 | 11)
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let Some(object) = self.read_catalog_record(&catalog_id_key(2, object_id))? else {
            return Ok(None);
        };
        if object.family_code != 2 {
            return Ok(None);
        }
        let object_kind = catalog_u16(&object, 1)?;
        let (schema_id, target_kind) = match object_kind {
            1 => (object_id, 1),
            3 => {
                let Some(schema_id) = catalog_optional_id(&object, 2)? else {
                    return Err(TransactionError::CorruptCatalog);
                };
                (schema_id, 3)
            }
            _ => return Ok(None),
        };
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        let mut authorized = roles.contains(&crate::bootstrap::database_admin_role_id());
        if target_kind == 1 {
            authorized |= subjects.contains(&catalog_id(&object, 4)?);
        } else {
            let schema = self
                .read_catalog_record(&catalog_id_key(2, schema_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            let table = self
                .read_catalog_record(&catalog_id_key(5, object_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if schema.family_code != 2 || catalog_u16(&schema, 1)? != 1 {
                return Err(TransactionError::CorruptCatalog);
            }
            if table.family_code != 5 || catalog_id(&table, 1)? != schema_id {
                return Err(TransactionError::CorruptCatalog);
            }
            authorized |= subjects.contains(&catalog_id(&schema, 4)?)
                || subjects.contains(&catalog_id(&object, 4)?);
        }
        let targets: &[([u8; 16], bool)] = if target_kind == 1 {
            &[(schema_id, true)]
        } else {
            &[(schema_id, true), (object_id, false)]
        };
        for (target, schema_scope) in targets {
            if authorized {
                break;
            }
            let grants = self
                .pinned_snapshot()?
                .lookup_grants_for_object(*target)
                .map_err(|error| match error {
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(grants.len())?;
            for grant in grants {
                let key = &grant.logical_key;
                if grant.family_code != 12
                    || key.len() != 59
                    || key[0..2] != 12u16.to_be_bytes()
                    || key[2] != 1
                    || key[19] != 1
                    || key[20..36] != *target
                    || key[36] != 1
                    || key[37..39] != privilege_code.to_be_bytes()
                    || key[39] != 1
                    || key[40..42] != 1u16.to_be_bytes()
                    || key[42] != 1
                    || key[43..59] != if *schema_scope { *target } else { object_id }
                {
                    continue;
                }
                if catalog_u64(&grant, 2)? == 0 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let grantee: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if grantee == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if subjects.contains(&grantee) {
                    authorized = true;
                    break;
                }
            }
        }
        if !authorized {
            return Ok(None);
        }
        let snapshot = self.pinned_snapshot()?;
        let name_index = snapshot
            .read_transaction()
            .open_table(PRINCIPAL_NAMES)
            .map_err(|error| TransactionError::Storage(error.to_string()))?;
        let Some(identity_id) = name_index
            .get(name.as_bytes())
            .map_err(|error| TransactionError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let identity_id: [u8; 16] = identity_id
            .value()
            .try_into()
            .map_err(|_| TransactionError::CorruptCatalog)?;
        if identity_id == [0; 16] {
            return Err(TransactionError::CorruptCatalog);
        }
        let principal = self
            .read_catalog_record(&catalog_id_key(10, identity_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if principal.family_code != 10 {
            return Err(TransactionError::CorruptCatalog);
        }
        let expected_kind = match kind {
            crate::metadata_discovery::AdministrativeIdentityKind::User => Some(1),
            crate::metadata_discovery::AdministrativeIdentityKind::Role => Some(2),
            crate::metadata_discovery::AdministrativeIdentityKind::Either => None,
        };
        if let Some(expected_kind) = expected_kind
            && catalog_u16(&principal, 1)? != expected_kind
        {
            return Err(TransactionError::CorruptCatalog);
        }
        if catalog_field(&principal, 2)? != name.as_bytes() {
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(Some(identity_id))
    }

    /// Resolve a policy name only inside a table for which this actor has
    /// policy DDL authority. The mutating drop operation rechecks authority.
    pub(crate) fn resolve_catalog_policy_name(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        name: &str,
        is_cell: Option<bool>,
    ) -> Result<Option<[u8; 16]>, TransactionError> {
        if name.is_empty()
            || name.len() > 128
            || !name.as_bytes()[0].is_ascii_lowercase() && name.as_bytes()[0] != b'_'
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
            || schema_id == [0; 16]
            || table_id == [0; 16]
        {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        let is_database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        let Some(schema) = self.read_catalog_record(&catalog_id_key(2, schema_id))? else {
            return Ok(None);
        };
        let Some(table) = self.read_catalog_record(&catalog_id_key(5, table_id))? else {
            return Ok(None);
        };
        let Some(schema_object) = self.read_catalog_record(&catalog_id_key(2, schema_id))? else {
            return Err(TransactionError::CorruptCatalog);
        };
        let Some(table_object) = self.read_catalog_record(&catalog_id_key(2, table_id))? else {
            return Err(TransactionError::CorruptCatalog);
        };
        if schema.family_code != 2
            || catalog_u16(&schema, 1)? != 1
            || table.family_code != 5
            || catalog_id(&table, 1)? != schema_id
            || schema_object.family_code != 2
            || catalog_u16(&schema_object, 1)? != 1
            || table_object.family_code != 2
            || catalog_u16(&table_object, 1)? != 3
        {
            return Ok(None);
        }
        let mut authorized = is_database_admin
            || subjects.contains(&catalog_id(&schema_object, 4)?)
            || subjects.contains(&catalog_id(&table_object, 4)?);
        for object_id in [schema_id, table_id] {
            if authorized {
                break;
            }
            let grants = self
                .pinned_snapshot()?
                .lookup_grants_for_object(object_id)
                .map_err(|error| match error {
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(grants.len())?;
            for grant in grants {
                let key = &grant.logical_key;
                if grant.family_code != 12
                    || key.len() != 59
                    || key[0..2] != 12u16.to_be_bytes()
                    || key[2] != 1
                    || key[19] != 1
                    || key[20..36] != object_id
                    || key[36] != 1
                    || key[37..39] != 10u16.to_be_bytes()
                    || key[39] != 1
                    || key[40..42] != 1u16.to_be_bytes()
                    || key[42] != 1
                    || key[43..59] != object_id
                    || catalog_u64(&grant, 2)? == 0
                {
                    continue;
                }
                let grantee: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if grantee == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if subjects.contains(&grantee) {
                    authorized = true;
                    break;
                }
            }
        }
        if !authorized {
            return Ok(None);
        }
        for policy_id in self.scan_policy_ids_for_table(table_id)? {
            let policy = self
                .read_catalog_record(&catalog_id_key(13, policy_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if policy.family_code != 13 || catalog_id(&policy, 1)? != table_id {
                return Err(TransactionError::CorruptCatalog);
            }
            let policy_kind = catalog_u16(&policy, 2)?;
            if !matches!(policy_kind, 1 | 2) {
                return Err(TransactionError::CorruptCatalog);
            }
            if let Some(is_cell) = is_cell {
                let expected_kind = if is_cell { 2 } else { 1 };
                if policy_kind != expected_kind {
                    continue;
                }
            }
            let object = self
                .read_catalog_record(&catalog_id_key(2, policy_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if object.family_code != 2 || catalog_u16(&object, 1)? != 10 {
                return Err(TransactionError::CorruptCatalog);
            }
            if catalog_field(&object, 3)? == name.as_bytes() {
                return Ok(Some(policy_id));
            }
        }
        Ok(None)
    }

    /// List schemas visible for authenticated control operations. This entry
    /// point is separate from invocation discovery so DDL name binding never
    /// widens executor or SQL query grants.
    pub(crate) fn list_catalog_administrative_schemas(
        &mut self,
    ) -> Result<Vec<VisibleSchema>, TransactionError> {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        let records = if database_admin {
            self.scan_catalog_family(2)?
        } else {
            let mut owner_ids = roles.clone();
            owner_ids.insert(self.principal_id);
            let mut owner_ids = owner_ids.into_iter().collect::<Vec<_>>();
            owner_ids.sort_unstable();
            let mut object_ids = Vec::new();
            for owner_id in owner_ids {
                object_ids.extend(self.scan_object_ids_for_owner(owner_id)?);
            }
            object_ids.sort_unstable();
            if object_ids.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(TransactionError::CorruptCatalog);
            }
            if object_ids.is_empty() {
                // Older low-level snapshots may contain object rows without
                // the derived owner index; retain the bounded compatibility
                // path when no indexed owner entry is found.
                self.scan_catalog_family(2)?
            } else {
                object_ids
                    .into_iter()
                    .map(|object_id| {
                        self.read_catalog_record(&catalog_id_key(2, object_id))?
                            .ok_or(TransactionError::CorruptCatalog)
                    })
                    .collect::<Result<Vec<_>, _>>()?
            }
        };
        let mut schemas = Vec::new();
        let mut encoded_bytes = 0usize;
        for record in records {
            if record.family_code != 2 {
                continue;
            }
            if record.logical_key.len() != 19
                || record.logical_key[2] != 1
                || record.logical_key[..2] != 2u16.to_be_bytes()
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let object_id: [u8; 16] = record.logical_key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let object_kind = catalog_u16(&record, 1)?;
            let owner_id = catalog_id(&record, 4)?;
            if !database_admin && owner_id != self.principal_id && !roles.contains(&owner_id) {
                continue;
            }
            let schema_id = match object_kind {
                1 => object_id,
                3 if !database_admin => {
                    let Some(schema_id) = catalog_optional_id(&record, 2)? else {
                        return Err(TransactionError::CorruptCatalog);
                    };
                    let schema_object = self
                        .read_catalog_record(&catalog_id_key(2, schema_id))?
                        .ok_or(TransactionError::CorruptCatalog)?;
                    if schema_object.family_code != 2 || catalog_u16(&schema_object, 1)? != 1 {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    schema_id
                }
                _ => continue,
            };
            let schema = self.read_discovery_schema(schema_id)?;
            encoded_bytes = encoded_bytes
                .saturating_add(16)
                .saturating_add(schema.name.len());
            if schemas.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            schemas.push(schema);
        }
        if !database_admin {
            let mut authority_subjects = roles.clone();
            authority_subjects.insert(self.principal_id);
            let mut discovered_schema_ids = schemas
                .iter()
                .map(|schema: &VisibleSchema| schema.schema_id)
                .collect::<HashSet<_>>();
            for grant in self.scan_catalog_family(12)? {
                if grant.logical_key.len() != 59
                    || grant.logical_key[2] != 1
                    || grant.logical_key[19] != 1
                    || grant.logical_key[36] != 1
                    || grant.logical_key[39] != 1
                    || grant.logical_key[42] != 1
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let privilege_code =
                    u16::from_be_bytes([grant.logical_key[37], grant.logical_key[38]]);
                if !matches!(privilege_code, 7..=9)
                    || grant.logical_key[40..42] != 1u16.to_be_bytes()
                {
                    continue;
                }
                let grantee_id: [u8; 16] = grant.logical_key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if !authority_subjects.contains(&grantee_id) {
                    continue;
                }
                let object_id: [u8; 16] = grant.logical_key[20..36]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                let target_id: [u8; 16] = grant.logical_key[43..59]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if object_id == [0; 16]
                    || object_id != target_id
                    || grant.logical_key[36] != 1
                    || catalog_u64(&grant, 2)? == 0
                    || catalog_id(&grant, 1)? == [0; 16]
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let object = self
                    .read_catalog_record(&catalog_id_key(2, object_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if object.family_code != 2 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let schema_id = match (privilege_code, catalog_u16(&object, 1)?) {
                    (7, 1) => object_id,
                    (8, 1) => object_id,
                    (8 | 9, 3) => {
                        let Some(schema_id) = catalog_optional_id(&object, 2)? else {
                            return Err(TransactionError::CorruptCatalog);
                        };
                        schema_id
                    }
                    _ => continue,
                };
                let schema = self.read_discovery_schema(schema_id)?;
                if !discovered_schema_ids.insert(schema_id) {
                    continue;
                }
                encoded_bytes = encoded_bytes
                    .saturating_add(16)
                    .saturating_add(schema.name.len());
                if schemas.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                    return Err(TransactionError::InvalidMetadataRequest);
                }
                schemas.push(schema);
            }
        }
        schemas.push(VisibleSchema {
            schema_id: INFORMATION_SCHEMA_ID,
            name: "information_schema".to_owned(),
        });
        schemas.sort_unstable_by_key(|schema| schema.schema_id);
        schemas.dedup_by_key(|schema| schema.schema_id);
        Ok(schemas)
    }

    /// List relations in a schema visible to this control principal through
    /// ordinary discovery, schema ownership, policy DDL, or scoped table DDL.
    /// `None` means no relation is visible under any of those authorities.
    pub(crate) fn list_catalog_administrative_relations(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<Option<Vec<VisibleRelation>>, TransactionError> {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let schema_admin = self.has_schema_metadata_authority(schema_id, &roles)?;
        let has_schema_usage = if !schema_admin {
            let (_, schema_grants) = self.load_catalog_schema_usage_grants(self.principal_id)?;
            schema_grants.iter().any(|grant| {
                grant.operation == Operation::Usage
                    && grant.target == AccessTarget::Schema(schema_id)
            })
        } else {
            true
        };
        let mut relation_ids = self.scan_relation_ids_for_schema(schema_id)?;
        if relation_ids.is_empty() {
            for record in self.scan_catalog_family(5)? {
                if record.family_code != 5 {
                    return Err(TransactionError::CorruptCatalog);
                }
                if catalog_id(&record, 1)? == schema_id {
                    let relation_id = record
                        .logical_key
                        .get(3..19)
                        .ok_or(TransactionError::CorruptCatalog)?
                        .try_into()
                        .map_err(|_| TransactionError::CorruptCatalog)?;
                    relation_ids.push(relation_id);
                }
            }
        }
        relation_ids.sort_unstable();
        if relation_ids.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(TransactionError::CorruptCatalog);
        }
        let mut visible_relation_ids = if schema_admin {
            relation_ids.iter().copied().collect::<HashSet<_>>()
        } else if !has_schema_usage {
            HashSet::new()
        } else {
            let mut visible = HashSet::new();
            let mut continuation = None;
            loop {
                let Some(page) = self.list_catalog_authorized_relations(
                    schema_id,
                    crate::metadata_discovery::MAX_RELATIONS_PER_PAGE,
                    continuation,
                )?
                else {
                    return Ok(None);
                };
                visible.extend(
                    page.relations
                        .into_iter()
                        .map(|relation| relation.relation_id),
                );
                continuation = page.continuation;
                if continuation.is_none() {
                    break;
                }
            }
            visible
        };
        if !schema_admin {
            for relation_id in &relation_ids {
                let policy_authority = has_schema_usage
                    && self.has_catalog_policy_ddl_authority(schema_id, *relation_id, &roles)?;
                if policy_authority
                    || self.has_catalog_table_owner_authority(*relation_id, &roles)?
                    || self.current_principal_has_index_ddl_authority(*relation_id)?
                    || self.current_principal_has_alter_ddl_authority(*relation_id)?
                {
                    visible_relation_ids.insert(*relation_id);
                }
            }
        }
        if !schema_admin && !has_schema_usage && visible_relation_ids.is_empty() {
            return Ok(None);
        }
        let mut relations = Vec::new();
        let mut encoded_bytes = 0usize;
        for relation_id in relation_ids {
            if !visible_relation_ids.contains(&relation_id) {
                continue;
            }
            let relation = self.read_discovery_relation(schema_id, relation_id)?;
            encoded_bytes = encoded_bytes
                .saturating_add(16)
                .saturating_add(relation.name.len());
            if relations.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            relations.push(relation);
        }
        Ok(Some(relations))
    }

    /// Describe a relation's declared shape for ordinary discovery or an
    /// authorized DDL target, including MANAGE_GRANTS, INDEX, and ALTER
    /// authorities.
    pub(crate) fn describe_catalog_administrative_columns(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<VisibleColumn>>, TransactionError> {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let schema_admin = self.has_schema_metadata_authority(schema_id, &roles)?;
        let table_key = catalog_id_key(5, relation_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 {
            return Err(TransactionError::CorruptCatalog);
        }
        if catalog_id(&table, 1)? != schema_id {
            return Ok(None);
        }
        if !schema_admin {
            let (_, schema_grants) = self.load_catalog_schema_usage_grants(self.principal_id)?;
            let has_usage = schema_grants.iter().any(|grant| {
                grant.operation == Operation::Usage
                    && grant.target == AccessTarget::Schema(schema_id)
            });
            let has_policy_metadata = has_usage
                && self.has_catalog_policy_ddl_authority(schema_id, relation_id, &roles)?;
            if !has_policy_metadata
                && !self.has_catalog_table_owner_authority(relation_id, &roles)?
                && !self.current_principal_has_grant_ddl_authority(relation_id)?
                && !self.current_principal_has_index_ddl_authority(relation_id)?
                && !self.current_principal_has_alter_ddl_authority(relation_id)?
            {
                return Ok(None);
            }
        }
        self.read_discovery_relation(schema_id, relation_id)?;
        let schema = self.load_catalog_table_schema(relation_id)?;
        let mut columns = Vec::with_capacity(schema.columns.len());
        let mut encoded_bytes = 0usize;
        for ordinal in 0..schema.columns.len() {
            let column = self.read_visible_column(relation_id, &schema, ordinal)?;
            encoded_bytes = encoded_bytes
                .saturating_add(24)
                .saturating_add(column.name.len())
                .saturating_add(
                    crate::metadata_discovery::scalar_type_name(column.scalar_type).len(),
                );
            if columns.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            columns.push(column);
        }
        Ok(Some(columns))
    }

    /// List logical indexes for a relation when this control principal owns
    /// its schema or has scoped index DDL authority. Physical storage
    /// identifiers and membership records are never returned.
    pub(crate) fn list_catalog_administrative_indexes(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeIndex>>, TransactionError> {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let schema_admin = self.has_schema_metadata_authority(schema_id, &roles)?;
        let table_key = catalog_id_key(5, relation_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 || catalog_id(&table, 1)? != schema_id {
            return Ok(None);
        }
        if !schema_admin && !self.current_principal_has_index_ddl_authority(relation_id)? {
            return Ok(None);
        }
        self.read_discovery_relation(schema_id, relation_id)?;
        let schema = self.load_catalog_table_schema(relation_id)?;
        let indexes = self.load_catalog_indexes(&schema)?;
        let mut descriptions = Vec::with_capacity(indexes.len());
        let mut encoded_bytes = 0usize;
        for index in indexes {
            let object = self
                .read_catalog_record(&catalog_id_key(2, index.index_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if object.family_code != 2
                || catalog_u16(&object, 1)? != 7
                || catalog_optional_id(&object, 2)?
                    .is_some_and(|owner_schema| owner_schema != schema_id)
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let name = String::from_utf8(catalog_field(&object, 3)?.to_vec())
                .map_err(|_| TransactionError::CorruptCatalog)?;
            encoded_bytes = encoded_bytes
                .saturating_add(17)
                .saturating_add(index.column_ids.len().saturating_mul(16))
                .saturating_add(name.len());
            if descriptions.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            descriptions.push(crate::metadata_discovery::AdministrativeIndex {
                index_id: index.index_id,
                name,
                column_ids: index.column_ids,
                unique: index.unique,
            });
        }
        descriptions.sort_unstable_by_key(|index| index.index_id);
        Ok(Some(descriptions))
    }

    /// Describe a table's logical constraints to its schema/table owner, a
    /// table-scoped ALTER grantee, or a database administrator. CHECK
    /// expression bodies are validated but omitted. Foreign-key endpoints are
    /// returned only when the target schema is also administratively visible.
    pub(crate) fn list_catalog_administrative_constraints(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeConstraint>>, TransactionError>
    {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let table_key = catalog_id_key(5, relation_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 || catalog_id(&table, 1)? != schema_id {
            return Ok(None);
        }
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        let schema_authority = self.has_schema_metadata_authority(schema_id, &roles)?;
        let table_object = self
            .read_catalog_record(&catalog_id_key(2, relation_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if table_object.family_code != 2 || catalog_u16(&table_object, 1)? != 3 {
            return Err(TransactionError::CorruptCatalog);
        }
        let owner_authority = subjects.contains(&catalog_id(&table_object, 4)?);
        let alter_authority = self.current_principal_has_object_ddl_authority(relation_id, 8)?;
        if !schema_authority && !owner_authority && !alter_authority {
            return Ok(None);
        }
        self.read_discovery_relation(schema_id, relation_id)?;
        let schema = self.load_catalog_table_schema(relation_id)?;
        // Validate the complete logical definitions against the same pinned
        // catalog before projecting the secret-free administrative shape.
        self.load_catalog_indexes(&schema)?;
        self.load_catalog_check_constraints(&schema)?;

        let constraint_ids = self.scan_constraint_ids_for_table(relation_id)?;
        let records = if constraint_ids.is_empty() {
            self.scan_catalog_family(8)?
        } else {
            constraint_ids
                .into_iter()
                .map(|constraint_id| {
                    self.read_catalog_record(&catalog_id_key(8, constraint_id))?
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let mut descriptions = Vec::new();
        let mut encoded_bytes = 0usize;
        for record in records {
            if record.family_code != 8 || record.logical_key.len() != 19 {
                return Err(TransactionError::CorruptCatalog);
            }
            if catalog_id(&record, 1)? != relation_id {
                continue;
            }
            let constraint_id: [u8; 16] = record.logical_key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let object = self
                .read_catalog_record(&catalog_id_key(2, constraint_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            let name = std::str::from_utf8(catalog_field(&object, 3)?)
                .map_err(|_| TransactionError::CorruptCatalog)?
                .to_owned();
            let kind = match catalog_u16(&record, 2)? {
                1 => crate::metadata_discovery::AdministrativeConstraintKind::NotNull,
                2 => crate::metadata_discovery::AdministrativeConstraintKind::PrimaryKey,
                3 => crate::metadata_discovery::AdministrativeConstraintKind::Unique,
                4 => crate::metadata_discovery::AdministrativeConstraintKind::Check,
                5 => crate::metadata_discovery::AdministrativeConstraintKind::ForeignKey,
                _ => return Err(TransactionError::CorruptCatalog),
            };
            let column_ids = catalog_id_list(&record, 3)?;
            let mut referenced_table_id = None;
            let mut referenced_column_ids = Vec::new();
            if kind == crate::metadata_discovery::AdministrativeConstraintKind::ForeignKey {
                let referenced_constraint_id =
                    catalog_optional_id(&record, 5)?.ok_or(TransactionError::CorruptCatalog)?;
                let referenced_constraint = self
                    .read_catalog_record(&catalog_id_key(8, referenced_constraint_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if referenced_constraint.family_code != 8 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let target_table_id = catalog_id(&referenced_constraint, 1)?;
                let target_table = self
                    .read_catalog_record(&catalog_id_key(5, target_table_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if target_table.family_code != 5 {
                    return Err(TransactionError::CorruptCatalog);
                }
                let target_schema_id = catalog_id(&target_table, 1)?;
                if self.has_schema_metadata_authority(target_schema_id, &roles)? {
                    referenced_table_id = Some(target_table_id);
                    referenced_column_ids = catalog_id_list(&record, 6)?;
                }
            }
            encoded_bytes = encoded_bytes
                .saturating_add(18)
                .saturating_add(name.len())
                .saturating_add(kind.as_wire_name().len())
                .saturating_add(column_ids.len().saturating_mul(16))
                .saturating_add(1)
                .saturating_add(usize::from(referenced_table_id.is_some()).saturating_mul(16))
                .saturating_add(referenced_column_ids.len().saturating_mul(16));
            if descriptions.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            descriptions.push(crate::metadata_discovery::AdministrativeConstraint {
                constraint_id,
                name,
                kind,
                column_ids,
                referenced_table_id,
                referenced_column_ids,
            });
        }
        descriptions.sort_unstable_by_key(|constraint| constraint.constraint_id);
        Ok(Some(descriptions))
    }

    /// Describe direct table and column grants to a current schema owner or
    /// database administrator. Principal names and inherited grants are not
    /// part of this direct-provenance view.
    pub(crate) fn list_catalog_administrative_grants(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeGrant>>, TransactionError> {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        if !self.has_schema_metadata_authority(schema_id, &roles)? {
            return Ok(None);
        }
        let table_key = catalog_id_key(5, relation_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 || catalog_id(&table, 1)? != schema_id {
            return Ok(None);
        }
        self.read_discovery_relation(schema_id, relation_id)?;
        let schema = self.load_catalog_table_schema(relation_id)?;
        let columns = schema.column_ids.iter().copied().collect::<HashSet<_>>();
        let records = self
            .pinned_snapshot()?
            .lookup_grants_for_object(relation_id)
            .map_err(|error| match error {
                SnapshotError::Storage(error) => TransactionError::Storage(error),
                _ => TransactionError::CorruptCatalog,
            })?;
        self.charge_scan_work(records.len())?;
        let mut descriptions = Vec::with_capacity(records.len());
        let mut encoded_bytes = 0usize;
        for record in records {
            let key = &record.logical_key;
            if record.family_code != 12
                || key.len() != 59
                || key[0..2] != 12u16.to_be_bytes()
                || key[2] != 1
                || key[19] != 1
                || key[20..36] != relation_id
                || key[36] != 1
                || key[39] != 1
                || key[42] != 1
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let grantee_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let privilege_code = u16::from_be_bytes([key[37], key[38]]);
            let scope_code = u16::from_be_bytes([key[40], key[41]]);
            let target_id: [u8; 16] = key[43..59]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            if grantee_id == [0; 16] || target_id == [0; 16] {
                return Err(TransactionError::CorruptCatalog);
            }
            let grantor_id = catalog_id(&record, 1)?;
            if grantor_id == [0; 16] || catalog_u64(&record, 2)? == 0 {
                return Err(TransactionError::CorruptCatalog);
            }
            for principal_id in [grantee_id, grantor_id] {
                let principal = self
                    .read_catalog_record(&catalog_id_key(10, principal_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if principal.family_code != 10 {
                    return Err(TransactionError::CorruptCatalog);
                }
            }
            let privilege = match privilege_code {
                2 => crate::metadata_discovery::AdministrativeGrantPrivilege::Select,
                3 => crate::metadata_discovery::AdministrativeGrantPrivilege::Insert,
                4 => crate::metadata_discovery::AdministrativeGrantPrivilege::Update,
                5 => crate::metadata_discovery::AdministrativeGrantPrivilege::Delete,
                6 => crate::metadata_discovery::AdministrativeGrantPrivilege::References,
                8 => crate::metadata_discovery::AdministrativeGrantPrivilege::Alter,
                9 => crate::metadata_discovery::AdministrativeGrantPrivilege::Index,
                10 => crate::metadata_discovery::AdministrativeGrantPrivilege::ManagePolicies,
                11 => crate::metadata_discovery::AdministrativeGrantPrivilege::ManageGrants,
                _ => return Err(TransactionError::CorruptCatalog),
            };
            let target_scope = match scope_code {
                1 if target_id == relation_id => {
                    crate::metadata_discovery::AdministrativeGrantTargetScope::Object
                }
                2 if matches!(privilege_code, 2 | 3 | 4 | 6) && columns.contains(&target_id) => {
                    crate::metadata_discovery::AdministrativeGrantTargetScope::Column
                }
                _ => return Err(TransactionError::CorruptCatalog),
            };
            encoded_bytes = encoded_bytes
                .saturating_add(81)
                .saturating_add(privilege.as_wire_name().len())
                .saturating_add(target_scope.as_wire_name().len());
            if descriptions.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            descriptions.push(crate::metadata_discovery::AdministrativeGrant {
                grantee_id,
                grantor_id,
                privilege,
                target_scope,
                target_id,
            });
        }
        descriptions.sort_unstable_by_key(|grant| {
            let privilege = match grant.privilege {
                crate::metadata_discovery::AdministrativeGrantPrivilege::Select => 2,
                crate::metadata_discovery::AdministrativeGrantPrivilege::Insert => 3,
                crate::metadata_discovery::AdministrativeGrantPrivilege::Update => 4,
                crate::metadata_discovery::AdministrativeGrantPrivilege::Delete => 5,
                crate::metadata_discovery::AdministrativeGrantPrivilege::References => 6,
                crate::metadata_discovery::AdministrativeGrantPrivilege::Alter => 8,
                crate::metadata_discovery::AdministrativeGrantPrivilege::Index => 9,
                crate::metadata_discovery::AdministrativeGrantPrivilege::ManagePolicies => 10,
                crate::metadata_discovery::AdministrativeGrantPrivilege::ManageGrants => 11,
            };
            let scope = match grant.target_scope {
                crate::metadata_discovery::AdministrativeGrantTargetScope::Object => 1,
                crate::metadata_discovery::AdministrativeGrantTargetScope::Column => 2,
            };
            (grant.grantee_id, privilege, scope, grant.target_id)
        });
        Ok(Some(descriptions))
    }

    /// Return secret-free policy summaries for a table under current schema
    /// owner/database-administrator authority. Only database administrators
    /// receive policy recipient IDs; expression bodies and IDs are omitted.
    pub(crate) fn list_catalog_administrative_policies(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativePolicy>>, TransactionError>
    {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        if !self.has_schema_metadata_authority(schema_id, &roles)? {
            return Ok(None);
        }
        let table_key = catalog_id_key(5, relation_id);
        let Some(table) = self.read_catalog_record(&table_key)? else {
            return Ok(None);
        };
        if table.family_code != 5 || catalog_id(&table, 1)? != schema_id {
            return Ok(None);
        }
        self.read_discovery_relation(schema_id, relation_id)?;
        let schema = self.load_catalog_table_schema(relation_id)?;
        let is_database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        let policy_ids = self.scan_policy_ids_for_table(relation_id)?;
        let records = if policy_ids.is_empty() {
            self.scan_catalog_family(13)?
        } else {
            policy_ids
                .into_iter()
                .map(|policy_id| {
                    self.read_catalog_record(&catalog_id_key(13, policy_id))?
                        .ok_or(TransactionError::CorruptCatalog)
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let boolean_type_id = self.load_catalog_builtin_type_id(1)?;
        let mut descriptions = Vec::new();
        let mut encoded_bytes = 0usize;
        for record in records {
            if record.family_code != 13 || record.logical_key.len() != 19 {
                return Err(TransactionError::CorruptCatalog);
            }
            if catalog_id(&record, 1)? != relation_id {
                continue;
            }
            let policy_id: [u8; 16] = record.logical_key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            if policy_id == [0; 16] || catalog_u64(&record, 7)? == 0 {
                return Err(TransactionError::CorruptCatalog);
            }
            let kind = match catalog_u16(&record, 2)? {
                1 => crate::metadata_discovery::AdministrativePolicyKind::Row,
                2 => crate::metadata_discovery::AdministrativePolicyKind::Cell,
                _ => return Err(TransactionError::CorruptCatalog),
            };
            let operation = match catalog_u16(&record, 3)? {
                1 => crate::metadata_discovery::AdministrativePolicyOperation::Select,
                2 => crate::metadata_discovery::AdministrativePolicyOperation::Insert,
                3 => crate::metadata_discovery::AdministrativePolicyOperation::Update,
                4 => crate::metadata_discovery::AdministrativePolicyOperation::Delete,
                _ => return Err(TransactionError::CorruptCatalog),
            };
            let effect = match catalog_u16(&record, 4)? {
                1 => crate::metadata_discovery::AdministrativePolicyEffect::Allow,
                2 => crate::metadata_discovery::AdministrativePolicyEffect::Restrict,
                _ => return Err(TransactionError::CorruptCatalog),
            };
            if kind == crate::metadata_discovery::AdministrativePolicyKind::Cell
                && operation == crate::metadata_discovery::AdministrativePolicyOperation::Delete
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let using_id = catalog_optional_id(&record, 5)?;
            let check_id = catalog_optional_id(&record, 6)?;
            match operation {
                crate::metadata_discovery::AdministrativePolicyOperation::Select
                | crate::metadata_discovery::AdministrativePolicyOperation::Delete
                    if using_id.is_some() && check_id.is_none() => {}
                crate::metadata_discovery::AdministrativePolicyOperation::Insert
                    if using_id.is_none() && check_id.is_some() => {}
                crate::metadata_discovery::AdministrativePolicyOperation::Update
                    if using_id.is_some() && check_id.is_some() => {}
                _ => return Err(TransactionError::CorruptCatalog),
            }
            if let Some(expression_id) = using_id {
                self.read_bound_policy_expression(
                    expression_id,
                    ExpressionPurpose::PolicyUsing,
                    &schema,
                    boolean_type_id,
                )?;
            }
            if let Some(expression_id) = check_id {
                self.read_bound_policy_expression(
                    expression_id,
                    ExpressionPurpose::PolicyCheck,
                    &schema,
                    boolean_type_id,
                )?;
            }

            let target_records = self.scan_policy_targets_for_policy(policy_id)?;
            let mut recipients = Vec::new();
            let mut column_ids = Vec::new();
            let mut has_recipient = false;
            for target_record in target_records {
                let key = &target_record.logical_key;
                if target_record.family_code != 14
                    || key.len() != 39
                    || key[0..2] != 14u16.to_be_bytes()
                    || key[2] != 1
                    || key[3..19] != policy_id
                    || key[19] != 1
                    || key[22] != 1
                {
                    return Err(TransactionError::CorruptCatalog);
                }
                let target_kind = u16::from_be_bytes([key[20], key[21]]);
                let target_id: [u8; 16] = key[23..39]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if target_id == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                match target_kind {
                    1 | 2 => {
                        has_recipient = true;
                        let principal = self
                            .read_catalog_record(&catalog_id_key(10, target_id))?
                            .ok_or(TransactionError::CorruptCatalog)?;
                        let expected_kind = if target_kind == 1 { 1 } else { 2 };
                        let login_enabled = catalog_field(&principal, 3)?;
                        if principal.family_code != 10
                            || catalog_u16(&principal, 1)? != expected_kind
                            || catalog_u64(&principal, 4)? == 0
                            || !matches!(login_enabled, [0] | [1])
                            || (expected_kind == 2 && login_enabled != [0])
                        {
                            return Err(TransactionError::CorruptCatalog);
                        }
                        if is_database_admin {
                            recipients.push(
                                crate::metadata_discovery::AdministrativePolicyRecipient {
                                    kind: if target_kind == 1 {
                                        crate::metadata_discovery::AdministrativePolicyRecipientKind::User
                                    } else {
                                        crate::metadata_discovery::AdministrativePolicyRecipientKind::Role
                                    },
                                    principal_id: target_id,
                                },
                            );
                        }
                    }
                    3 => {
                        if kind != crate::metadata_discovery::AdministrativePolicyKind::Cell
                            || operation
                                == crate::metadata_discovery::AdministrativePolicyOperation::Delete
                            || !schema.column_ids.contains(&target_id)
                        {
                            return Err(TransactionError::CorruptCatalog);
                        }
                        let column = self
                            .read_catalog_record(&catalog_id_key(6, target_id))?
                            .ok_or(TransactionError::CorruptCatalog)?;
                        if catalog_id(&column, 1)? != relation_id {
                            return Err(TransactionError::CorruptCatalog);
                        }
                        column_ids.push(target_id);
                    }
                    _ => return Err(TransactionError::CorruptCatalog),
                }
            }
            if !has_recipient
                || recipients.is_empty() && is_database_admin
                || column_ids.is_empty()
                    && kind == crate::metadata_discovery::AdministrativePolicyKind::Cell
                || !column_ids.is_empty()
                    && kind == crate::metadata_discovery::AdministrativePolicyKind::Row
            {
                return Err(TransactionError::CorruptCatalog);
            }
            recipients
                .sort_unstable_by_key(|recipient| (recipient.kind as u8, recipient.principal_id));
            column_ids.sort_unstable();
            encoded_bytes = encoded_bytes
                .saturating_add(33)
                .saturating_add(column_ids.len().saturating_mul(16))
                .saturating_add(if is_database_admin {
                    recipients.len().saturating_mul(21)
                } else {
                    1
                });
            if descriptions.len() >= 4096 || encoded_bytes > 1024 * 1024 {
                return Err(TransactionError::InvalidMetadataRequest);
            }
            descriptions.push(crate::metadata_discovery::AdministrativePolicy {
                policy_id,
                kind,
                operation,
                effect,
                column_ids,
                recipients: is_database_admin.then_some(recipients),
            });
        }
        descriptions.sort_unstable_by_key(|policy| policy.policy_id);
        Ok(Some(descriptions))
    }

    pub(super) fn administrative_dependency_endpoint_visible(
        &mut self,
        endpoint_id: [u8; 16],
        roles: &HashSet<[u8; 16]>,
        is_database_admin: bool,
    ) -> Result<bool, TransactionError> {
        if endpoint_id == [0; 16] {
            return Ok(false);
        }
        let Some(mut object) = self.read_catalog_record(&catalog_id_key(2, endpoint_id))? else {
            if !is_database_admin {
                return Ok(false);
            }
            return Ok(self
                .read_catalog_record(&catalog_id_key(1, endpoint_id))?
                .is_some_and(|record| record.family_code == 1));
        };
        if object.family_code != 2 {
            return Err(TransactionError::CorruptCatalog);
        }
        let object_kind = catalog_u16(&object, 1)?;
        if matches!(object_kind, 8 | 9) {
            return Ok(is_database_admin);
        }
        let owning_table_id = match object_kind {
            4 | 6 | 7 | 10 => {
                let family = match object_kind {
                    4 => 6,
                    6 => 8,
                    7 => 9,
                    10 => 13,
                    _ => unreachable!(),
                };
                let typed = self
                    .read_catalog_record(&catalog_id_key(family, endpoint_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if typed.family_code != family {
                    return Err(TransactionError::CorruptCatalog);
                }
                Some(catalog_id(&typed, 1)?)
            }
            _ => None,
        };
        if let Some(table_id) = owning_table_id {
            let table = self
                .read_catalog_record(&catalog_id_key(5, table_id))?
                .ok_or(TransactionError::CorruptCatalog)?;
            if table.family_code != 5 {
                return Err(TransactionError::CorruptCatalog);
            }
            return self.has_schema_metadata_authority(catalog_id(&table, 1)?, roles);
        }
        let mut current_id = endpoint_id;
        let mut seen = HashSet::new();
        for _ in 0..32 {
            if !seen.insert(current_id) {
                return Err(TransactionError::CorruptCatalog);
            }
            let kind = catalog_u16(&object, 1)?;
            if kind == 1 {
                return self.has_schema_metadata_authority(current_id, roles);
            }
            if kind == 3 {
                let table = self
                    .read_catalog_record(&catalog_id_key(5, current_id))?
                    .ok_or(TransactionError::CorruptCatalog)?;
                if table.family_code != 5 {
                    return Err(TransactionError::CorruptCatalog);
                }
                return self.has_schema_metadata_authority(catalog_id(&table, 1)?, roles);
            }
            let Some(parent_id) = catalog_optional_id(&object, 2)? else {
                return Ok(false);
            };
            let Some(parent) = self.read_catalog_record(&catalog_id_key(2, parent_id))? else {
                return Err(TransactionError::CorruptCatalog);
            };
            if parent.family_code != 2 {
                return Err(TransactionError::CorruptCatalog);
            }
            current_id = parent_id;
            object = parent;
        }
        Err(TransactionError::CorruptCatalog)
    }

    /// Return bounded direct dependency edges touching a visible object. Edges
    /// with either endpoint outside the caller's administrative visibility are
    /// omitted, without returning hidden counts or names.
    pub(crate) fn list_catalog_administrative_dependencies(
        &mut self,
        object_id: [u8; 16],
    ) -> Result<Option<Vec<crate::metadata_discovery::AdministrativeDependency>>, TransactionError>
    {
        let roles = self.load_catalog_role_closure(self.principal_id)?;
        let is_database_admin = roles.contains(&crate::bootstrap::database_admin_role_id());
        if !self.administrative_dependency_endpoint_visible(object_id, &roles, is_database_admin)? {
            return Ok(None);
        }
        const EDGE_LIMIT: usize = 4097;
        let snapshot = self.pinned_snapshot()?;
        let outgoing = snapshot
            .lookup_dependencies_for_dependent(object_id, EDGE_LIMIT)
            .map_err(|error| match error {
                SnapshotError::Storage(error) => TransactionError::Storage(error),
                SnapshotError::RecordLimitExceeded => TransactionError::InvalidMetadataRequest,
                _ => TransactionError::CorruptCatalog,
            })?;
        let incoming = snapshot
            .lookup_dependents_for_referenced(object_id, EDGE_LIMIT)
            .map_err(|error| match error {
                SnapshotError::Storage(error) => TransactionError::Storage(error),
                SnapshotError::RecordLimitExceeded => TransactionError::InvalidMetadataRequest,
                _ => TransactionError::CorruptCatalog,
            })?;
        self.charge_scan_work(outgoing.len().saturating_add(incoming.len()))?;
        let mut edges = HashSet::new();
        for record in outgoing.into_iter().chain(incoming) {
            let key = &record.logical_key;
            if record.family_code != 15
                || key.len() != 39
                || key[0..2] != 15u16.to_be_bytes()
                || key[2] != 1
                || key[19] != 1
                || key[36] != 1
            {
                return Err(TransactionError::CorruptCatalog);
            }
            let dependent_id: [u8; 16] = key[3..19]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let referenced_id: [u8; 16] = key[20..36]
                .try_into()
                .map_err(|_| TransactionError::CorruptCatalog)?;
            let kind = u16::from_be_bytes([key[37], key[38]]);
            if dependent_id == [0; 16] || referenced_id == [0; 16] || !matches!(kind, 1..=5) {
                return Err(TransactionError::CorruptCatalog);
            }
            if self.administrative_dependency_endpoint_visible(
                dependent_id,
                &roles,
                is_database_admin,
            )? && self.administrative_dependency_endpoint_visible(
                referenced_id,
                &roles,
                is_database_admin,
            )? {
                edges.insert((dependent_id, referenced_id, kind));
            }
        }
        if edges.len() > 4096 {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        let mut descriptions = edges
            .into_iter()
            .map(|(dependent_id, referenced_id, kind)| {
                let kind = match kind {
                    1 => crate::metadata_discovery::AdministrativeDependencyKind::OwnedChild,
                    2 => {
                        crate::metadata_discovery::AdministrativeDependencyKind::ExpressionReference
                    }
                    3 => crate::metadata_discovery::AdministrativeDependencyKind::ForeignKeyTarget,
                    4 => crate::metadata_discovery::AdministrativeDependencyKind::BackingIndex,
                    5 => crate::metadata_discovery::AdministrativeDependencyKind::Authority,
                    _ => unreachable!("dependency kind checked above"),
                };
                crate::metadata_discovery::AdministrativeDependency {
                    dependent_id,
                    referenced_id,
                    kind,
                }
            })
            .collect::<Vec<_>>();
        descriptions
            .sort_unstable_by_key(|edge| (edge.dependent_id, edge.referenced_id, edge.kind as u8));
        let encoded_bytes = descriptions.iter().fold(0usize, |total, edge| {
            total.saturating_add(35 + edge.kind.as_wire_name().len())
        });
        if encoded_bytes > 1024 * 1024 {
            return Err(TransactionError::InvalidMetadataRequest);
        }
        Ok(Some(descriptions))
    }

    pub(super) fn has_schema_metadata_authority(
        &mut self,
        schema_id: [u8; 16],
        roles: &HashSet<[u8; 16]>,
    ) -> Result<bool, TransactionError> {
        let Some(object) = self.read_catalog_record(&catalog_id_key(2, schema_id))? else {
            return Ok(false);
        };
        if object.family_code != 2 || catalog_u16(&object, 1)? != 1 {
            return Err(TransactionError::CorruptCatalog);
        }
        if roles.contains(&crate::bootstrap::database_admin_role_id()) {
            self.read_discovery_schema(schema_id)?;
            return Ok(true);
        }
        self.read_discovery_schema(schema_id)?;
        let owner_id = catalog_id(&object, 4)?;
        Ok(owner_id == self.principal_id || roles.contains(&owner_id))
    }

    pub(super) fn has_catalog_table_owner_authority(
        &mut self,
        table_id: [u8; 16],
        roles: &HashSet<[u8; 16]>,
    ) -> Result<bool, TransactionError> {
        let Some(object) = self.read_catalog_record(&catalog_id_key(2, table_id))? else {
            return Ok(false);
        };
        if object.family_code != 2 || catalog_u16(&object, 1)? != 3 {
            return Ok(false);
        }
        let owner_id = catalog_id(&object, 4)?;
        Ok(owner_id == self.principal_id || roles.contains(&owner_id))
    }

    pub(super) fn current_principal_has_grant_ddl_authority(
        &mut self,
        object_id: [u8; 16],
    ) -> Result<bool, TransactionError> {
        self.current_principal_has_object_ddl_authority(object_id, 11)
    }

    pub(super) fn current_principal_has_index_ddl_authority(
        &mut self,
        object_id: [u8; 16],
    ) -> Result<bool, TransactionError> {
        self.current_principal_has_object_ddl_authority(object_id, 9)
    }

    pub(super) fn current_principal_has_alter_ddl_authority(
        &mut self,
        object_id: [u8; 16],
    ) -> Result<bool, TransactionError> {
        self.current_principal_has_object_ddl_authority(object_id, 8)
    }

    pub(super) fn current_principal_has_object_ddl_authority(
        &mut self,
        object_id: [u8; 16],
        privilege_code: u16,
    ) -> Result<bool, TransactionError> {
        let principal = self
            .read_catalog_record(&catalog_id_key(10, self.principal_id))?
            .ok_or(TransactionError::CorruptCatalog)?;
        if principal.family_code != 10 || catalog_u16(&principal, 1)? != 1 {
            return Err(TransactionError::CorruptCatalog);
        }
        let name = std::str::from_utf8(catalog_field(&principal, 2)?)
            .map_err(|_| TransactionError::CorruptCatalog)?;
        let Some(identity_id) = self.resolve_catalog_object_ddl_identity_name(
            object_id,
            name,
            crate::metadata_discovery::AdministrativeIdentityKind::User,
            privilege_code,
        )?
        else {
            return Ok(false);
        };
        if identity_id != self.principal_id {
            return Err(TransactionError::CorruptCatalog);
        }
        Ok(true)
    }

    pub(super) fn has_catalog_policy_ddl_authority(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        roles: &HashSet<[u8; 16]>,
    ) -> Result<bool, TransactionError> {
        let Some(schema_object) = self.read_catalog_record(&catalog_id_key(2, schema_id))? else {
            return Ok(false);
        };
        let Some(table_record) = self.read_catalog_record(&catalog_id_key(5, table_id))? else {
            return Ok(false);
        };
        let Some(table_object) = self.read_catalog_record(&catalog_id_key(2, table_id))? else {
            return Err(TransactionError::CorruptCatalog);
        };
        if schema_object.family_code != 2
            || catalog_u16(&schema_object, 1)? != 1
            || table_record.family_code != 5
            || catalog_id(&table_record, 1)? != schema_id
            || table_object.family_code != 2
            || catalog_u16(&table_object, 1)? != 3
        {
            return Ok(false);
        }
        let mut subjects = roles.clone();
        subjects.insert(self.principal_id);
        if roles.contains(&crate::bootstrap::database_admin_role_id()) {
            return Ok(true);
        }
        if subjects.contains(&catalog_id(&schema_object, 4)?)
            || subjects.contains(&catalog_id(&table_object, 4)?)
        {
            return Ok(true);
        }
        for object_id in [schema_id, table_id] {
            let grants = self
                .pinned_snapshot()?
                .lookup_grants_for_object(object_id)
                .map_err(|error| match error {
                    SnapshotError::Storage(error) => TransactionError::Storage(error),
                    _ => TransactionError::CorruptCatalog,
                })?;
            self.charge_scan_work(grants.len())?;
            for grant in grants {
                let key = &grant.logical_key;
                if grant.family_code != 12
                    || key.len() != 59
                    || key[0..2] != 12u16.to_be_bytes()
                    || key[2] != 1
                    || key[19] != 1
                    || key[20..36] != object_id
                    || key[36] != 1
                    || key[37..39] != 10u16.to_be_bytes()
                    || key[39] != 1
                    || key[40..42] != 1u16.to_be_bytes()
                    || key[42] != 1
                    || key[43..59] != object_id
                    || catalog_u64(&grant, 2)? == 0
                {
                    continue;
                }
                let grantee: [u8; 16] = key[3..19]
                    .try_into()
                    .map_err(|_| TransactionError::CorruptCatalog)?;
                if grantee == [0; 16] {
                    return Err(TransactionError::CorruptCatalog);
                }
                if subjects.contains(&grantee) {
                    if catalog_id(&grant, 1)? == [0; 16] {
                        return Err(TransactionError::CorruptCatalog);
                    }
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}
