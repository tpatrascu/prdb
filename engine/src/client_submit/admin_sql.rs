use super::codec::*;
use super::response::*;
use super::*;

impl AdminClient {
    /// Create an ordinary or unique B-tree index from qualified SQL names.
    /// Relation and column names are resolved through authenticated metadata;
    /// storage remains the authority for authorization and index validation.
    pub fn execute_create_index_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_create_index(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_not_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        self.create_index_from_sql_statement(statement)
    }

    /// Execute `CREATE [UNIQUE] INDEX IF NOT EXISTS`. Existing indexes are
    /// administrator-only no-ops; absent indexes use the regular typed path.
    pub fn execute_create_index_if_not_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminIndexResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_create_index(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_not_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        match self.resolve_index_sql_name(&statement.schema, &statement.name) {
            Ok(_) => {
                self.list_runtime_invocations_page(1, None)?;
                Ok(None)
            }
            Err(ClientSubmitError::InvalidAdministrativeRequest) => {
                self.create_index_from_sql_statement(statement).map(Some)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn create_index_from_sql_statement(
        &mut self,
        statement: crate::ddl_sql::CreateIndexSql,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
        let mut cursor = None;
        let mut visible_columns = Vec::new();
        loop {
            let page = self.list_columns_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            visible_columns.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let column_ids = statement
            .columns
            .iter()
            .map(|name| {
                visible_columns
                    .iter()
                    .find(|column| &column.name == name)
                    .map(|column| column.column_id)
                    .ok_or(ClientSubmitError::InvalidAdministrativeRequest)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.create_index(table_id, &statement.name, &column_ids, statement.unique)
    }

    pub(super) fn resolve_index_sql_name(
        &mut self,
        schema_name: &str,
        index_name: &str,
    ) -> Result<([u8; 16], crate::metadata_discovery::AdministrativeIndex), ClientSubmitError> {
        let schema_id = self.resolve_schema_sql_name(schema_name)?;
        let mut relation_cursor = None;
        let mut matched_index = None;
        loop {
            let relations = self.list_relations_page(
                schema_id,
                DEFAULT_METADATA_PAGE_SIZE,
                relation_cursor.as_deref(),
            )?;
            let next_relation_cursor = relations.next_cursor;
            for relation in relations.records {
                let mut index_cursor = None;
                loop {
                    let indexes = self.list_indexes_page(
                        schema_id,
                        relation.relation_id,
                        DEFAULT_METADATA_PAGE_SIZE,
                        index_cursor.as_deref(),
                    )?;
                    let next_index_cursor = indexes.next_cursor;
                    for index in indexes.records {
                        if index.name == index_name
                            && matched_index
                                .replace((relation.relation_id, index))
                                .is_some()
                        {
                            return Err(ClientSubmitError::InvalidResponse);
                        }
                    }
                    index_cursor = next_index_cursor;
                    if index_cursor.is_none() {
                        break;
                    }
                }
            }
            relation_cursor = next_relation_cursor;
            if relation_cursor.is_none() {
                break;
            }
        }
        matched_index.ok_or(ClientSubmitError::InvalidAdministrativeRequest)
    }

    /// Rebuild a schema-qualified index after resolving its logical definition
    /// through authenticated administrative metadata.
    pub fn execute_rebuild_index_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_rebuild_index(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let (_, index) = self.resolve_index_sql_name(&statement.schema, &statement.name)?;
        self.rebuild_index(index.index_id, &index.column_ids, index.unique)
    }

    /// Atomically replace a named index's ordered key columns and uniqueness
    /// profile after binding the new columns through authenticated metadata.
    pub fn execute_alter_index_rebuild_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_alter_index_rebuild(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let (table_id, index) = self.resolve_index_sql_name(&statement.schema, &statement.name)?;
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let mut cursor = None;
        let mut visible_columns = Vec::new();
        loop {
            let page = self.list_columns_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            visible_columns.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let column_ids = statement
            .columns
            .iter()
            .map(|name| {
                visible_columns
                    .iter()
                    .find(|column| &column.name == name)
                    .map(|column| column.column_id)
                    .ok_or(ClientSubmitError::InvalidAdministrativeRequest)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.rebuild_index(index.index_id, &column_ids, statement.unique)
    }

    /// Rename a schema-qualified index after resolving its stable ID through
    /// authenticated administrative metadata.
    pub fn execute_rename_index_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_rename_index(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let (_, index) = self.resolve_index_sql_name(&statement.schema, &statement.name)?;
        self.rename_index(index.index_id, &statement.new_name)
    }

    /// Drop a schema-qualified index after resolving its generated ID through
    /// authenticated administrative metadata.
    pub fn execute_drop_index_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_index(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let (_, index) = self.resolve_index_sql_name(&statement.schema, &statement.name)?;
        self.drop_index(index.index_id)
    }

    /// Execute `DROP INDEX IF EXISTS`. An unresolved target is suppressed only
    /// after the server confirms database-administrator authority.
    pub fn execute_drop_index_if_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminIndexResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_index(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        match self.resolve_index_sql_name(&statement.schema, &statement.name) {
            Ok((_, index)) => self.drop_index(index.index_id).map(Some),
            Err(ClientSubmitError::InvalidAdministrativeRequest) => {
                self.list_runtime_invocations_page(1, None)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Create a user or role from bounded SQL through the existing typed ADMIN
    /// operations. User creation does not issue a credential.
    pub fn execute_create_identity_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_create_identity(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_not_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        self.create_identity_from_statement(&statement)
    }

    /// Execute `CREATE USER|ROLE IF NOT EXISTS`. Existing identities are a
    /// successful no-op only after current database-administrator authority
    /// is confirmed; absent identities use the ordinary typed create path.
    pub fn execute_create_identity_if_not_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminIdentityResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_create_identity(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_not_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let identity_kind = match statement.kind {
            crate::ddl_sql::CreateIdentityKind::User => {
                crate::metadata_discovery::AdministrativeIdentityKind::User
            }
            crate::ddl_sql::CreateIdentityKind::Role => {
                crate::metadata_discovery::AdministrativeIdentityKind::Role
            }
        };
        match self.resolve_identity_sql_name(&statement.name, identity_kind) {
            Ok(_) => {
                self.list_runtime_invocations_page(1, None)?;
                Ok(None)
            }
            Err(ClientSubmitError::InvalidAdministrativeRequest) => {
                self.create_identity_from_statement(&statement).map(Some)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn create_identity_from_statement(
        &mut self,
        statement: &crate::ddl_sql::CreateIdentitySql,
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        match statement.kind {
            crate::ddl_sql::CreateIdentityKind::User if statement.login_enabled => {
                match statement.workload_profile.as_deref() {
                    Some(profile) => self.create_user_with_profile(&statement.name, profile),
                    None => self.create_user(&statement.name),
                }
            }
            crate::ddl_sql::CreateIdentityKind::User => {
                match statement.workload_profile.as_deref() {
                    Some(profile) => {
                        self.create_user_no_login_with_profile(&statement.name, profile)
                    }
                    None => self.create_user_no_login(&statement.name),
                }
            }
            crate::ddl_sql::CreateIdentityKind::Role => self.create_role(&statement.name),
        }
    }

    /// Rename a user or role through SQL, resolving its current catalog name
    /// through the authenticated, non-enumerating identity lookup.
    pub fn execute_rename_identity_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_rename_identity(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let kind = match statement.kind {
            crate::ddl_sql::CreateIdentityKind::User => "user",
            crate::ddl_sql::CreateIdentityKind::Role => "role",
        };
        let payload = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.identities.resolve\",\"arguments\":{{\"name\":\"{}\",\"kind\":\"{kind}\"}}}}",
            statement.old_name
        );
        let response = self.transport.exchange(10, payload.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 1 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let identity_id = response_id(&fields, "identity_id")
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        match statement.kind {
            crate::ddl_sql::CreateIdentityKind::User => {
                self.rename_user(identity_id, &statement.new_name)
            }
            crate::ddl_sql::CreateIdentityKind::Role => {
                self.rename_role(identity_id, &statement.new_name)
            }
        }
    }

    /// Drop a user or role through SQL after resolving its stable ID. Storage
    /// independently applies RESTRICT dependency checks and revalidates authority.
    pub fn execute_drop_identity_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_identity(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let identity_id = self.resolve_identity_sql_name(
            &statement.name,
            match statement.kind {
                crate::ddl_sql::CreateIdentityKind::User => {
                    crate::metadata_discovery::AdministrativeIdentityKind::User
                }
                crate::ddl_sql::CreateIdentityKind::Role => {
                    crate::metadata_discovery::AdministrativeIdentityKind::Role
                }
            },
        )?;
        match statement.kind {
            crate::ddl_sql::CreateIdentityKind::User => self.drop_user(identity_id),
            crate::ddl_sql::CreateIdentityKind::Role => self.drop_role(identity_id),
        }
    }

    /// Drop a user or role conditionally. An unresolved identity is suppressed
    /// only after current database-administrator authority is confirmed.
    pub fn execute_drop_identity_if_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminOperationResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_identity(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let kind = match statement.kind {
            crate::ddl_sql::CreateIdentityKind::User => {
                crate::metadata_discovery::AdministrativeIdentityKind::User
            }
            crate::ddl_sql::CreateIdentityKind::Role => {
                crate::metadata_discovery::AdministrativeIdentityKind::Role
            }
        };
        match self.resolve_identity_sql_name(&statement.name, kind) {
            Ok(identity_id) => match statement.kind {
                crate::ddl_sql::CreateIdentityKind::User => self.drop_user(identity_id).map(Some),
                crate::ddl_sql::CreateIdentityKind::Role => self.drop_role(identity_id).map(Some),
            },
            Err(ClientSubmitError::InvalidAdministrativeRequest) => {
                self.list_runtime_invocations_page(1, None)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Change a user's LOGIN state through SQL. Disabling LOGIN revokes all
    /// outstanding credentials in the existing atomic storage operation.
    pub fn execute_set_user_login_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_set_user_login(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let payload = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.identities.resolve\",\"arguments\":{{\"name\":\"{}\",\"kind\":\"user\"}}}}",
            statement.name
        );
        let response = self.transport.exchange(10, payload.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 1 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let user_id = response_id(&fields, "identity_id")
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        self.set_user_login(user_id, statement.enabled)
    }

    pub fn execute_set_user_workload_profile_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_set_user_workload_profile(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let user_id = self.resolve_identity_sql_name(
            &statement.user_name,
            crate::metadata_discovery::AdministrativeIdentityKind::User,
        )?;
        self.execute_admin_operation(
            "users.set_workload_profile",
            format!(
                "{{\"user_id\":\"{}\",\"profile_name\":\"{}\"}}",
                lower_hex(&user_id),
                statement.profile_name
            ),
            None,
        )
        .map(|(result, _)| result)
    }

    pub fn execute_create_workload_profile_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_create_workload_profile(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let settings = statement
            .settings
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        let args = match statement.base_profile {
            Some(base) => format!(
                "{{\"name\":\"{}\",\"base_profile\":\"{}\",\"settings\":\"{}\"}}",
                statement.name, base, settings
            ),
            None => format!(
                "{{\"name\":\"{}\",\"settings\":\"{}\"}}",
                statement.name, settings
            ),
        };
        self.execute_admin_operation("workload_profiles.create", args, Some("profile_id"))
            .map(|(result, _)| result)
    }

    pub fn execute_alter_workload_profile_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_alter_workload_profile(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let settings = statement
            .settings
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(",");
        self.execute_admin_operation(
            "workload_profiles.alter",
            format!(
                "{{\"name\":\"{}\",\"settings\":\"{}\"}}",
                statement.name, settings
            ),
            None,
        )
        .map(|(result, _)| result)
    }

    pub fn execute_rename_workload_profile_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_rename_workload_profile(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        self.execute_admin_operation(
            "workload_profiles.rename",
            format!(
                "{{\"old_name\":\"{}\",\"new_name\":\"{}\"}}",
                statement.old_name, statement.new_name
            ),
            None,
        )
        .map(|(result, _)| result)
    }

    pub fn execute_drop_workload_profile_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_workload_profile(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        self.execute_admin_operation(
            "workload_profiles.drop",
            format!("{{\"name\":\"{}\"}}", statement.name),
            None,
        )
        .map(|(result, _)| result)
    }

    /// Grant or revoke one role membership using authenticated name resolution
    /// followed by the existing typed membership operation.
    pub fn execute_role_membership_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_role_membership(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let role_id = self.resolve_identity_sql_name(
            &statement.role_name,
            crate::metadata_discovery::AdministrativeIdentityKind::Role,
        )?;
        let member_id = self.resolve_identity_sql_name(
            &statement.member_name,
            if statement.member_is_role {
                crate::metadata_discovery::AdministrativeIdentityKind::Role
            } else {
                crate::metadata_discovery::AdministrativeIdentityKind::User
            },
        )?;
        if statement.grant {
            self.grant_role(member_id, role_id)
        } else {
            self.revoke_role(member_id, role_id)
        }
    }

    /// Grant or revoke one database-scoped system privilege by identity name.
    pub fn execute_system_privilege_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_system_privilege(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let grantee_id = self.resolve_identity_sql_name(
            &statement.grantee_name,
            if statement.grantee_is_role {
                crate::metadata_discovery::AdministrativeIdentityKind::Role
            } else {
                crate::metadata_discovery::AdministrativeIdentityKind::User
            },
        )?;
        let privilege = match statement.privilege.as_str() {
            "manage_users" => AdminSystemPrivilege::ManageUsers,
            "manage_roles" => AdminSystemPrivilege::ManageRoles,
            "create_schema" => AdminSystemPrivilege::CreateSchema,
            _ => return Err(ClientSubmitError::InvalidAdministrativeRequest),
        };
        if statement.grant {
            self.grant_system_privilege(grantee_id, privilege)
        } else {
            self.revoke_system_privilege(grantee_id, privilege)
        }
    }

    /// Grant or revoke one object-scoped privilege on a schema or table.
    pub fn execute_object_privilege_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_object_privilege(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let grant_target = self.resolve_grant_target_sql_name(
            &statement.schema_name,
            statement.table_name.as_deref(),
            &statement.grantee_name,
            if statement.grantee_is_role {
                crate::metadata_discovery::AdministrativeIdentityKind::Role
            } else {
                crate::metadata_discovery::AdministrativeIdentityKind::User
            },
        )?;
        let schema_id = grant_target.schema_id;
        let object_id = grant_target.object_id;
        let grantee_id = grant_target.identity_id;
        let privileges = statement
            .privileges
            .iter()
            .map(|privilege| match privilege.as_str() {
                "usage" => Ok(AdminObjectPrivilege::Usage),
                "select" => Ok(AdminObjectPrivilege::Select),
                "insert" => Ok(AdminObjectPrivilege::Insert),
                "update" => Ok(AdminObjectPrivilege::Update),
                "delete" => Ok(AdminObjectPrivilege::Delete),
                "references" => Ok(AdminObjectPrivilege::References),
                "create" => Ok(AdminObjectPrivilege::Create),
                "alter" => Ok(AdminObjectPrivilege::Alter),
                "index" => Ok(AdminObjectPrivilege::Index),
                "manage_policies" => Ok(AdminObjectPrivilege::ManagePolicies),
                "manage_grants" => Ok(AdminObjectPrivilege::ManageGrants),
                _ => Err(ClientSubmitError::InvalidAdministrativeRequest),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let expands_table_data_grant = statement.table_name.is_some()
            && privileges.iter().any(|privilege| {
                matches!(
                    privilege,
                    AdminObjectPrivilege::Select
                        | AdminObjectPrivilege::Insert
                        | AdminObjectPrivilege::Update
                        | AdminObjectPrivilege::References
                )
            });
        let target = if !statement.column_names.is_empty() {
            let table_id = object_id;
            let mut column_ids = Vec::with_capacity(statement.column_names.len());
            for column_name in &statement.column_names {
                let mut cursor = None;
                let mut matched = None;
                loop {
                    let page = self.list_columns_page(
                        schema_id,
                        table_id,
                        DEFAULT_METADATA_PAGE_SIZE,
                        cursor.as_deref(),
                    )?;
                    if let Some(column) = page
                        .records
                        .iter()
                        .find(|column| column.name == *column_name)
                    {
                        matched = Some(column.column_id);
                        break;
                    }
                    cursor = page.next_cursor;
                    if cursor.is_none() {
                        break;
                    }
                }
                column_ids.push(matched.ok_or(ClientSubmitError::InvalidAdministrativeRequest)?);
            }
            if column_ids.len() == 1 {
                AdminObjectGrantTarget::Column(column_ids[0])
            } else {
                AdminObjectGrantTarget::Columns(column_ids)
            }
        } else if expands_table_data_grant {
            AdminObjectGrantTarget::AllColumns
        } else {
            AdminObjectGrantTarget::Object
        };
        let has_data_privilege = privileges.iter().any(|privilege| {
            matches!(
                privilege,
                AdminObjectPrivilege::Select
                    | AdminObjectPrivilege::Insert
                    | AdminObjectPrivilege::Update
                    | AdminObjectPrivilege::References
            )
        });
        let has_object_privilege = privileges.iter().any(|privilege| {
            !matches!(
                privilege,
                AdminObjectPrivilege::Select
                    | AdminObjectPrivilege::Insert
                    | AdminObjectPrivilege::Update
                    | AdminObjectPrivilege::References
            )
        });
        let has_table_data_privilege = statement.table_name.is_some()
            && statement.column_names.is_empty()
            && privileges.iter().any(|privilege| {
                matches!(
                    privilege,
                    AdminObjectPrivilege::Select
                        | AdminObjectPrivilege::Insert
                        | AdminObjectPrivilege::Update
                )
            });
        if (has_data_privilege && has_object_privilege) || has_table_data_privilege {
            let mut entries = Vec::new();
            for privilege in privileges {
                let is_data = matches!(
                    privilege,
                    AdminObjectPrivilege::Select
                        | AdminObjectPrivilege::Insert
                        | AdminObjectPrivilege::Update
                        | AdminObjectPrivilege::References
                );
                if has_table_data_privilege
                    && matches!(
                        privilege,
                        AdminObjectPrivilege::Select
                            | AdminObjectPrivilege::Insert
                            | AdminObjectPrivilege::Update
                    )
                {
                    entries.push(AdminObjectGrantEntry {
                        target: AdminObjectGrantTarget::Object,
                        privilege,
                    });
                }
                entries.push(AdminObjectGrantEntry {
                    target: if is_data {
                        target.clone()
                    } else {
                        AdminObjectGrantTarget::Object
                    },
                    privilege,
                });
            }
            return self.change_object_grant_entries(
                if statement.grant {
                    "objects.grant"
                } else {
                    "objects.revoke"
                },
                grantee_id,
                object_id,
                entries,
            );
        }
        self.change_object_privileges(
            if statement.grant {
                "objects.grant"
            } else {
                "objects.revoke"
            },
            grantee_id,
            object_id,
            privileges,
            target,
        )
    }

    pub(super) fn resolve_identity_sql_name(
        &mut self,
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<[u8; 16], ClientSubmitError> {
        let kind = match kind {
            crate::metadata_discovery::AdministrativeIdentityKind::User => "user",
            crate::metadata_discovery::AdministrativeIdentityKind::Role => "role",
            crate::metadata_discovery::AdministrativeIdentityKind::Either => "either",
        };
        let payload = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.identities.resolve\",\"arguments\":{{\"name\":\"{name}\",\"kind\":\"{kind}\"}}}}"
        );
        let response = self.transport.exchange(10, payload.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 1 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        response_id(&fields, "identity_id")
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)
    }

    pub(super) fn resolve_alter_identity_sql_name(
        &mut self,
        object_id: [u8; 16],
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<[u8; 16], ClientSubmitError> {
        let kind = match kind {
            crate::metadata_discovery::AdministrativeIdentityKind::User => "user",
            crate::metadata_discovery::AdministrativeIdentityKind::Role => "role",
            crate::metadata_discovery::AdministrativeIdentityKind::Either => "either",
        };
        let payload = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.alter_identities.resolve\",\"arguments\":{{\"object_id\":\"{}\",\"name\":\"{name}\",\"kind\":\"{kind}\"}}}}",
            lower_hex(&object_id)
        );
        let response = self.transport.exchange(10, payload.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 1 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        response_id(&fields, "identity_id")
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)
    }

    pub(super) fn resolve_policy_identity_sql_name(
        &mut self,
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
        schema_id: [u8; 16],
        table_id: [u8; 16],
    ) -> Result<[u8; 16], ClientSubmitError> {
        let kind = match kind {
            crate::metadata_discovery::AdministrativeIdentityKind::User => "user",
            crate::metadata_discovery::AdministrativeIdentityKind::Role => "role",
            crate::metadata_discovery::AdministrativeIdentityKind::Either => "either",
        };
        let payload = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.policy_identities.resolve\",\"arguments\":{{\"schema_id\":\"{}\",\"table_id\":\"{}\",\"name\":{},\"kind\":\"{kind}\"}}}}",
            lower_hex(&schema_id),
            lower_hex(&table_id),
            json_quote(name),
        );
        let response = self.transport.exchange(10, payload.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 1 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        response_id(&fields, "identity_id")
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)
    }

    pub(super) fn resolve_grant_target_sql_name(
        &mut self,
        schema_name: &str,
        table_name: Option<&str>,
        name: &str,
        kind: crate::metadata_discovery::AdministrativeIdentityKind,
    ) -> Result<crate::metadata_discovery::AdministrativeGrantIdentityResolution, ClientSubmitError>
    {
        let kind = match kind {
            crate::metadata_discovery::AdministrativeIdentityKind::User => "user",
            crate::metadata_discovery::AdministrativeIdentityKind::Role => "role",
            crate::metadata_discovery::AdministrativeIdentityKind::Either => "either",
        };
        let payload = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.grant_identities.resolve\",\"arguments\":{{\"schema_name\":{},{}\"name\":{},\"kind\":\"{kind}\"}}}}",
            json_quote(schema_name),
            table_name
                .map(|value| format!("\"table_name\":{},", json_quote(value)))
                .unwrap_or_default(),
            json_quote(name),
        );
        let response = self.transport.exchange(10, payload.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 3 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let schema_id = response_id(&fields, "schema_id")
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let object_id = response_id(&fields, "object_id")
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let identity_id = response_id(&fields, "identity_id")
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        Ok(
            crate::metadata_discovery::AdministrativeGrantIdentityResolution {
                schema_id,
                object_id,
                identity_id,
            },
        )
    }

    pub(super) fn resolve_policy_sql_name(
        &mut self,
        name: &str,
        is_cell: Option<bool>,
        schema_id: [u8; 16],
        table_id: [u8; 16],
    ) -> Result<[u8; 16], ClientSubmitError> {
        let kind = match is_cell {
            Some(true) => "cell",
            Some(false) => "row",
            None => "any",
        };
        let payload = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"metadata.policies.resolve\",\"arguments\":{{\"schema_id\":\"{}\",\"table_id\":\"{}\",\"name\":{},\"kind\":\"{kind}\"}}}}",
            lower_hex(&schema_id),
            lower_hex(&table_id),
            json_quote(name),
        );
        let response = self.transport.exchange(10, payload.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 1 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        response_id(&fields, "policy_id")
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)
    }

    pub fn execute_create_policy_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminPolicyResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_create_policy(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
        let mut cursor = None;
        let mut columns = Vec::new();
        loop {
            let page = self.list_columns_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            columns.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let kind = if statement.is_cell {
            AdminPolicyKind::Cell
        } else {
            AdminPolicyKind::Row
        };
        let operation = match statement.operation.as_str() {
            "select" => AdminPolicyOperation::Select,
            "insert" => AdminPolicyOperation::Insert,
            "update" => AdminPolicyOperation::Update,
            "delete" => AdminPolicyOperation::Delete,
            _ => return Err(ClientSubmitError::InvalidAdministrativeRequest),
        };
        let effect = match statement.effect.as_str() {
            "allow" => AdminPolicyEffect::Allow,
            "restrict" => AdminPolicyEffect::Restrict,
            _ => return Err(ClientSubmitError::InvalidAdministrativeRequest),
        };
        let mut recipients = Vec::with_capacity(statement.recipients.len());
        for recipient in &statement.recipients {
            let kind = if recipient.is_role {
                crate::metadata_discovery::AdministrativeIdentityKind::Role
            } else {
                crate::metadata_discovery::AdministrativeIdentityKind::User
            };
            let id =
                self.resolve_policy_identity_sql_name(&recipient.name, kind, schema_id, table_id)?;
            recipients.push(if recipient.is_role {
                AdminPolicyRecipient::Role(id)
            } else {
                AdminPolicyRecipient::User(id)
            });
        }
        let mut target_column_ids = Vec::with_capacity(statement.columns.len());
        for column_name in &statement.columns {
            let column = columns
                .iter()
                .find(|column| &column.name == column_name)
                .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
            target_column_ids.push(column.column_id);
        }
        let bind_expression = |client: &mut Self,
                               expression: &crate::ddl_sql::CheckExpressionSql|
         -> Result<crate::authorization::Expr, ClientSubmitError> {
            crate::ddl_sql::bind_policy_expression(
                expression,
                |name| {
                    columns
                        .iter()
                        .find(|column| column.name == name)
                        .map(|column| (column.ordinal as usize, column.scalar_type))
                },
                |name| {
                    client
                        .resolve_policy_identity_sql_name(
                            name,
                            crate::metadata_discovery::AdministrativeIdentityKind::Role,
                            schema_id,
                            table_id,
                        )
                        .ok()
                },
            )
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)
        };
        let using_expression = statement
            .using_expression
            .as_ref()
            .map(|expression| bind_expression(self, expression))
            .transpose()?;
        let with_check_expression = statement
            .with_check_expression
            .as_ref()
            .map(|expression| bind_expression(self, expression))
            .transpose()?;
        let request = AdminCreatePolicyRequest {
            table_id,
            name: &statement.name,
            kind,
            operation,
            effect,
            recipients: &recipients,
            target_column_ids: &target_column_ids,
            using_expression: using_expression.as_ref(),
            with_check_expression: with_check_expression.as_ref(),
        };
        if statement.replace_existing {
            let replaced_policy_id =
                self.resolve_policy_sql_name(&statement.name, None, schema_id, table_id)?;
            self.replace_policy(request, replaced_policy_id)
        } else {
            self.create_policy(request)
        }
    }

    pub fn execute_drop_policy_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_policy(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
        let policy_id = self.resolve_policy_sql_name(
            &statement.name,
            Some(statement.is_cell),
            schema_id,
            table_id,
        )?;
        self.drop_policy(policy_id)
    }

    /// Execute `DROP ROW|CELL POLICY IF EXISTS`. An unresolved target is
    /// suppressed only after the server confirms database-administrator
    /// authority; a resolved policy retains its ordinary owner/manager checks.
    pub fn execute_drop_policy_if_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminOperationResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_policy(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let resolved = (|| {
            let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
            let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
            self.resolve_policy_sql_name(
                &statement.name,
                Some(statement.is_cell),
                schema_id,
                table_id,
            )
        })();
        match resolved {
            Ok(policy_id) => self.drop_policy(policy_id).map(Some),
            Err(ClientSubmitError::InvalidAdministrativeRequest) => {
                self.list_runtime_invocations_page(1, None)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Execute the supported SQL `CREATE TABLE` subset through the
    /// authenticated administrative control channel. The SQL is parsed locally
    /// into the same typed operation as `create_table`; storage remains the
    /// authority for authorization and catalog validation.
    pub fn execute_create_table_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminTableResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_create_table(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_not_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        self.create_table_from_sql_statement(statement)
    }

    /// Execute `CREATE TABLE IF NOT EXISTS`. An existing relation is a
    /// database-administrator-only no-op; absent relations use the ordinary
    /// typed CREATE TABLE path.
    pub fn execute_create_table_if_not_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminTableResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_create_table(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_not_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        match self.resolve_table_sql_name(&statement.schema, &statement.table) {
            Ok(_) => {
                self.list_runtime_invocations_page(1, None)?;
                Ok(None)
            }
            Err(ClientSubmitError::InvalidAdministrativeRequest) => {
                self.create_table_from_sql_statement(statement).map(Some)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn create_table_from_sql_statement(
        &mut self,
        statement: crate::ddl_sql::CreateTableSql,
    ) -> Result<AdminTableResult, ClientSubmitError> {
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let checks = statement
            .check_constraints
            .iter()
            .map(|expression| {
                crate::ddl_sql::bind_check_expression(expression, |name| {
                    statement
                        .columns
                        .iter()
                        .position(|column| column.name == name)
                        .map(|ordinal| (ordinal, statement.columns[ordinal].scalar_type))
                })
                .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut foreign_keys = Vec::with_capacity(statement.foreign_keys.len());
        for foreign_key in &statement.foreign_keys {
            if foreign_key.referenced_schema == statement.schema
                && foreign_key.referenced_table == statement.table
            {
                let local_key_exists = statement
                    .primary_key_columns
                    .as_ref()
                    .is_some_and(|columns| columns == &foreign_key.referenced_columns)
                    || statement
                        .unique_constraints
                        .iter()
                        .any(|columns| columns == &foreign_key.referenced_columns);
                if !local_key_exists {
                    return Err(ClientSubmitError::InvalidAdministrativeRequest);
                }
                foreign_keys.push(AdminTableForeignKeyDefinition {
                    source_columns: foreign_key.columns.clone(),
                    target_constraint_id: [0; 16],
                    target_columns: Some(foreign_key.referenced_columns.clone()),
                });
                continue;
            }
            let target_schema_id = self.resolve_schema_sql_name(&foreign_key.referenced_schema)?;
            let target_table_id = self.resolve_table_sql_name_in_schema(
                target_schema_id,
                &foreign_key.referenced_table,
            )?;
            let mut column_cursor = None;
            let mut target_columns = Vec::new();
            loop {
                let page = self.list_columns_page(
                    target_schema_id,
                    target_table_id,
                    DEFAULT_METADATA_PAGE_SIZE,
                    column_cursor.as_deref(),
                )?;
                target_columns.extend(page.records);
                column_cursor = page.next_cursor;
                if column_cursor.is_none() {
                    break;
                }
            }
            let target_column_ids = foreign_key
                .referenced_columns
                .iter()
                .map(|name| {
                    target_columns
                        .iter()
                        .find(|column| &column.name == name)
                        .map(|column| column.column_id)
                        .ok_or(ClientSubmitError::InvalidAdministrativeRequest)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut constraint_cursor = None;
            let mut target_constraints = Vec::new();
            loop {
                let page = self.list_constraints_page(
                    target_schema_id,
                    target_table_id,
                    DEFAULT_METADATA_PAGE_SIZE,
                    constraint_cursor.as_deref(),
                )?;
                target_constraints.extend(page.records);
                constraint_cursor = page.next_cursor;
                if constraint_cursor.is_none() {
                    break;
                }
            }
            let target_constraint_id = target_constraints
                .iter()
                .find(|constraint| {
                    matches!(
                        constraint.kind,
                        crate::metadata_discovery::AdministrativeConstraintKind::PrimaryKey
                            | crate::metadata_discovery::AdministrativeConstraintKind::Unique
                    ) && constraint.column_ids == target_column_ids
                })
                .map(|constraint| constraint.constraint_id)
                .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
            foreign_keys.push(AdminTableForeignKeyDefinition {
                source_columns: foreign_key.columns.clone(),
                target_constraint_id,
                target_columns: None,
            });
        }
        let check_names = if statement.check_constraint_names.iter().any(Option::is_some) {
            statement.check_constraint_names.as_slice()
        } else {
            &[]
        };
        let unique_names = if statement
            .unique_constraint_names
            .iter()
            .any(Option::is_some)
        {
            statement.unique_constraint_names.as_slice()
        } else {
            &[]
        };
        let foreign_key_names = if statement.foreign_key_names.iter().any(Option::is_some) {
            statement.foreign_key_names.as_slice()
        } else {
            &[]
        };
        self.create_table_with_named_constraints_and_foreign_keys(
            schema_id,
            &statement.table,
            &statement.columns,
            statement.primary_key_columns.as_deref(),
            statement.primary_key_name,
            &statement.unique_constraints,
            unique_names,
            &checks,
            check_names,
            &foreign_keys,
            foreign_key_names,
        )
    }

    /// Execute bounded CREATE SCHEMA SQL through the authenticated control
    /// channel, resolving explicitly typed user and role owners as needed.
    pub fn execute_create_schema_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_create_schema(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_not_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        self.create_schema_from_sql_statement(statement)
    }

    /// Execute `CREATE SCHEMA IF NOT EXISTS`. Existing schemas are an
    /// administrator-only no-op; absent schemas use the ordinary create path.
    pub fn execute_create_schema_if_not_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminIdentityResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_create_schema(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_not_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        match self.resolve_schema_sql_name(&statement.schema) {
            Ok(_) => {
                self.list_runtime_invocations_page(1, None)?;
                Ok(None)
            }
            Err(ClientSubmitError::InvalidAdministrativeRequest) => {
                self.create_schema_from_sql_statement(statement).map(Some)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn create_schema_from_sql_statement(
        &mut self,
        statement: crate::ddl_sql::CreateSchemaSql,
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        let owner_id = match statement.owner {
            crate::ddl_sql::CreateSchemaOwnerSql::CurrentUser => None,
            crate::ddl_sql::CreateSchemaOwnerSql::Id(owner_id) => Some(owner_id),
            crate::ddl_sql::CreateSchemaOwnerSql::Identity { name, kind } => {
                let kind = match kind {
                    crate::ddl_sql::SqlIdentityKind::User => {
                        crate::metadata_discovery::AdministrativeIdentityKind::User
                    }
                    crate::ddl_sql::SqlIdentityKind::Role => {
                        crate::metadata_discovery::AdministrativeIdentityKind::Role
                    }
                    crate::ddl_sql::SqlIdentityKind::Either => {
                        crate::metadata_discovery::AdministrativeIdentityKind::Either
                    }
                };
                Some(self.resolve_identity_sql_name(&name, kind)?)
            }
        };
        self.create_schema_with_owner(&statement.schema, owner_id)
    }

    pub fn execute_rename_schema_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_rename_schema(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        self.rename_schema(schema_id, &statement.new_name)
    }

    pub fn execute_schema_owner_transfer_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_schema_owner_transfer(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let kind = match statement.owner_kind {
            crate::ddl_sql::SqlIdentityKind::User => {
                crate::metadata_discovery::AdministrativeIdentityKind::User
            }
            crate::ddl_sql::SqlIdentityKind::Role => {
                crate::metadata_discovery::AdministrativeIdentityKind::Role
            }
            crate::ddl_sql::SqlIdentityKind::Either => {
                crate::metadata_discovery::AdministrativeIdentityKind::Either
            }
        };
        let owner_id =
            self.resolve_alter_identity_sql_name(schema_id, &statement.owner_name, kind)?;
        self.set_schema_owner(schema_id, owner_id)
    }

    pub fn execute_drop_schema_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_schema(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        self.drop_schema(schema_id)
    }

    /// Execute `DROP SCHEMA IF EXISTS`. An unresolved schema is suppressed only
    /// after the server confirms database-administrator authority.
    pub fn execute_drop_schema_if_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminOperationResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_schema(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        match self.resolve_schema_sql_name(&statement.schema) {
            Ok(schema_id) => self.drop_schema(schema_id).map(Some),
            Err(ClientSubmitError::InvalidAdministrativeRequest) => {
                self.list_runtime_invocations_page(1, None)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    /// Add a PRIMARY KEY or UNIQUE constraint using qualified SQL names and
    /// the existing typed, separately authorized constraint operation.
    pub fn execute_add_key_constraint_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminKeyConstraintResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_add_key_constraint(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
        let mut cursor = None;
        let mut visible_columns = Vec::new();
        loop {
            let page = self.list_columns_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            visible_columns.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let mut column_ids = Vec::with_capacity(statement.columns.len());
        for column_name in &statement.columns {
            let column = visible_columns
                .iter()
                .find(|column| &column.name == column_name)
                .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
            if statement.kind == crate::ddl_sql::KeyConstraintKind::PrimaryKey && column.nullable {
                return Err(ClientSubmitError::InvalidAdministrativeRequest);
            }
            column_ids.push(column.column_id);
        }
        match statement.kind {
            crate::ddl_sql::KeyConstraintKind::PrimaryKey => {
                if let Some(name) = statement.name.as_deref() {
                    self.create_named_primary_key(table_id, &column_ids, name)
                } else {
                    self.create_primary_key(table_id, &column_ids)
                }
            }
            crate::ddl_sql::KeyConstraintKind::Unique => {
                if let Some(name) = statement.name.as_deref() {
                    self.create_named_unique_constraint(table_id, &column_ids, name)
                } else {
                    self.create_unique_constraint(table_id, &column_ids)
                }
            }
        }
    }

    /// Add a foreign key by resolving both relations and the referenced
    /// PRIMARY KEY/UNIQUE constraint through authenticated administrative
    /// metadata before invoking the typed operation.
    pub fn execute_add_foreign_key_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminForeignKeyResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_add_foreign_key(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let source_schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let source_table_id =
            self.resolve_table_sql_name_in_schema(source_schema_id, &statement.table)?;
        let target_schema_id = self.resolve_schema_sql_name(&statement.referenced_schema)?;
        let target_table_id =
            self.resolve_table_sql_name_in_schema(target_schema_id, &statement.referenced_table)?;

        fn read_columns(
            client: &mut AdminClient,
            schema_id: [u8; 16],
            table_id: [u8; 16],
        ) -> Result<Vec<crate::metadata_discovery::VisibleColumn>, ClientSubmitError> {
            let mut cursor = None;
            let mut columns = Vec::new();
            loop {
                let page = client.list_columns_page(
                    schema_id,
                    table_id,
                    DEFAULT_METADATA_PAGE_SIZE,
                    cursor.as_deref(),
                )?;
                columns.extend(page.records);
                cursor = page.next_cursor;
                if cursor.is_none() {
                    return Ok(columns);
                }
            }
        }

        let source_columns = read_columns(self, source_schema_id, source_table_id)?;
        let target_columns = read_columns(self, target_schema_id, target_table_id)?;
        let resolve_ids =
            |names: &[String], columns: &[crate::metadata_discovery::VisibleColumn]| {
                names
                    .iter()
                    .map(|name| {
                        columns
                            .iter()
                            .find(|column| &column.name == name)
                            .map(|column| column.column_id)
                            .ok_or(ClientSubmitError::InvalidAdministrativeRequest)
                    })
                    .collect::<Result<Vec<_>, _>>()
            };
        let source_column_ids = resolve_ids(&statement.columns, &source_columns)?;
        let target_column_ids = resolve_ids(&statement.referenced_columns, &target_columns)?;

        let mut cursor = None;
        let mut target_constraints = Vec::new();
        loop {
            let page = self.list_constraints_page(
                target_schema_id,
                target_table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            target_constraints.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let target_constraint_id = target_constraints
            .iter()
            .find(|constraint| {
                matches!(
                    constraint.kind,
                    crate::metadata_discovery::AdministrativeConstraintKind::PrimaryKey
                        | crate::metadata_discovery::AdministrativeConstraintKind::Unique
                ) && constraint.column_ids == target_column_ids
            })
            .map(|constraint| constraint.constraint_id)
            .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;

        if let Some(name) = statement.name.as_deref() {
            self.create_named_foreign_key(
                source_table_id,
                &source_column_ids,
                target_constraint_id,
                name,
            )
        } else {
            self.create_foreign_key(source_table_id, &source_column_ids, target_constraint_id)
        }
    }

    /// Drop a constraint identified by its explicit name or generated ID.
    pub fn execute_drop_constraint_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_constraint(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let (constraint_id, kind) = self.resolve_drop_constraint_sql_target(&statement)?;
        Self::drop_constraint_sql_target(self, constraint_id, kind)
    }

    /// Execute `DROP CONSTRAINT IF EXISTS`. An unresolved constraint is
    /// suppressed only after the server confirms database-administrator authority.
    pub fn execute_drop_constraint_if_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminOperationResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_constraint(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        match self.resolve_drop_constraint_sql_target(&statement) {
            Ok((constraint_id, kind)) => {
                Self::drop_constraint_sql_target(self, constraint_id, kind).map(Some)
            }
            Err(ClientSubmitError::InvalidAdministrativeRequest) => {
                self.list_runtime_invocations_page(1, None)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn resolve_drop_constraint_sql_target(
        &mut self,
        statement: &crate::ddl_sql::DropConstraintSql,
    ) -> Result<
        (
            [u8; 16],
            crate::metadata_discovery::AdministrativeConstraintKind,
        ),
        ClientSubmitError,
    > {
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
        let mut cursor = None;
        let mut found_kind = None;
        loop {
            let page = self.list_constraints_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            if let Some(constraint) = page.records.iter().find(|constraint| {
                statement.constraint_name.as_ref().map_or_else(
                    || constraint.constraint_id == statement.constraint_id,
                    |name| constraint.name == *name,
                )
            }) {
                found_kind = Some((constraint.constraint_id, constraint.kind));
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        found_kind.ok_or(ClientSubmitError::InvalidAdministrativeRequest)
    }

    pub(super) fn drop_constraint_sql_target(
        &mut self,
        constraint_id: [u8; 16],
        kind: crate::metadata_discovery::AdministrativeConstraintKind,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        match kind {
            crate::metadata_discovery::AdministrativeConstraintKind::PrimaryKey => {
                self.drop_primary_key(constraint_id)
            }
            crate::metadata_discovery::AdministrativeConstraintKind::Unique => {
                self.drop_unique_constraint(constraint_id)
            }
            crate::metadata_discovery::AdministrativeConstraintKind::Check => {
                self.drop_check_constraint(constraint_id)
            }
            crate::metadata_discovery::AdministrativeConstraintKind::ForeignKey => {
                self.drop_foreign_key(constraint_id)
            }
            crate::metadata_discovery::AdministrativeConstraintKind::NotNull => {
                Err(ClientSubmitError::InvalidAdministrativeRequest)
            }
        }
    }

    /// Add a bounded CHECK expression using qualified SQL names and the
    /// existing typed, separately authorized constraint operation.
    pub fn execute_add_check_constraint_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminCheckConstraintResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_add_check_constraint(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
        let mut cursor = None;
        let mut visible_columns = Vec::new();
        loop {
            let page = self.list_columns_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            visible_columns.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let mut dependencies = std::collections::BTreeSet::new();
        let expression = crate::ddl_sql::bind_check_expression(&statement.expression, |name| {
            visible_columns
                .iter()
                .find(|column| column.name == name)
                .map(|column| (column.ordinal as usize, column.scalar_type))
        })
        .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        fn collect_fields(
            expression: &crate::authorization::Expr,
            columns: &[crate::metadata_discovery::VisibleColumn],
            dependencies: &mut std::collections::BTreeSet<[u8; 16]>,
        ) -> Result<(), ClientSubmitError> {
            use crate::authorization::Expr;
            match expression {
                Expr::Field(ordinal) => {
                    let column = columns
                        .iter()
                        .find(|column| column.ordinal as usize == *ordinal)
                        .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
                    dependencies.insert(column.column_id);
                }
                Expr::Equal(left, right)
                | Expr::NotEqual(left, right)
                | Expr::Less(left, right)
                | Expr::LessEqual(left, right)
                | Expr::Greater(left, right)
                | Expr::GreaterEqual(left, right)
                | Expr::Add(left, right)
                | Expr::Subtract(left, right)
                | Expr::Multiply(left, right)
                | Expr::Modulo(left, right)
                | Expr::Divide(left, right) => {
                    collect_fields(left, columns, dependencies)?;
                    collect_fields(right, columns, dependencies)?;
                }
                Expr::And(expressions) | Expr::Or(expressions) => {
                    for expression in expressions {
                        collect_fields(expression, columns, dependencies)?;
                    }
                }
                Expr::Not(inner) | Expr::IsNull(inner) | Expr::Negate(inner) => {
                    collect_fields(inner, columns, dependencies)?;
                }
                Expr::Literal(_) => {}
                Expr::CurrentPrincipalId | Expr::HasRole(_) => {
                    return Err(ClientSubmitError::InvalidAdministrativeRequest);
                }
            }
            Ok(())
        }
        collect_fields(&expression, &visible_columns, &mut dependencies)?;
        let dependencies = dependencies.into_iter().collect::<Vec<_>>();
        if let Some(name) = statement.name.as_deref() {
            self.create_named_check_constraint(table_id, name, &expression, &dependencies)
        } else {
            self.create_check_constraint(table_id, &expression, &dependencies)
        }
    }

    /// Set or drop NOT NULL using qualified SQL names and the existing typed
    /// column-alteration operation.
    pub fn execute_alter_column_nullability_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_alter_column_nullability(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
        let mut cursor = None;
        let mut visible_columns = Vec::new();
        loop {
            let page = self.list_columns_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            visible_columns.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let column = visible_columns
            .iter()
            .find(|column| column.name == statement.column)
            .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
        self.alter_column_nullability(table_id, column.column_id, statement.nullable)
    }

    /// Widen a VARCHAR column through the existing authenticated typed DDL path.
    pub fn execute_widen_varchar_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_widen_varchar(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
        let mut cursor = None;
        let mut visible_columns = Vec::new();
        loop {
            let page = self.list_columns_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            visible_columns.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let column = visible_columns
            .iter()
            .find(|column| column.name == statement.column)
            .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
        match column.scalar_type {
            crate::tuple::ScalarType::Varchar { max_chars } if statement.max_chars > max_chars => {
                self.widen_varchar_column(table_id, column.column_id, statement.max_chars)
            }
            _ => Err(ClientSubmitError::InvalidAdministrativeRequest),
        }
    }

    /// Set or drop a typed column default after resolving its declared type
    /// and nullability through authenticated metadata.
    pub fn execute_alter_column_default_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_alter_column_default(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
        let mut cursor = None;
        let mut visible_columns = Vec::new();
        loop {
            let page = self.list_columns_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            visible_columns.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let column = visible_columns
            .iter()
            .find(|column| column.name == statement.column)
            .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
        let Some(literal) = statement.default else {
            return self.drop_column_default(table_id, column.column_id);
        };
        let default = crate::ddl_sql::bind_default_literal(&literal, column.scalar_type)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if matches!(default, AdminColumnDefault::Null) && !column.nullable {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        self.set_column_default(table_id, column.column_id, column.scalar_type, default)
    }

    /// Append one bounded SQL column definition through authenticated control
    /// metadata and the existing typed add-column operation.
    pub fn execute_add_column_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminTableColumnResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_add_column(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_not_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let table_id = self.resolve_table_sql_name(&statement.schema, &statement.table)?;
        self.add_column(table_id, &statement.column)
    }

    /// Execute `ADD COLUMN IF NOT EXISTS`. Existing columns are a no-op only
    /// after confirming database-administrator authority.
    pub fn execute_add_column_if_not_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminTableColumnResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_add_column(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_not_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
        let mut cursor = None;
        loop {
            let page = self.list_columns_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            if page
                .records
                .iter()
                .any(|column| column.name == statement.column.name)
            {
                self.list_runtime_invocations_page(1, None)?;
                return Ok(None);
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        self.add_column(table_id, &statement.column).map(Some)
    }

    /// Rename one declared column using qualified SQL names and the existing
    /// typed column-rename operation.
    pub fn execute_rename_column_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminTableColumnResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_rename_column(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let schema_id = self.resolve_schema_sql_name(&statement.schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, &statement.table)?;
        let mut cursor = None;
        let mut visible_columns = Vec::new();
        loop {
            let page = self.list_columns_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            visible_columns.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        let column = visible_columns
            .iter()
            .find(|column| column.name == statement.column)
            .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
        self.rename_column(table_id, column.column_id, &statement.new_name)
    }

    /// Drop one column with RESTRICT semantics using qualified SQL names and
    /// the existing typed column-drop operation.
    pub fn execute_drop_column_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminTableColumnResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_column(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let (table_id, column_id) = self.resolve_drop_column_sql_target(
            &statement.schema,
            &statement.table,
            &statement.column,
        )?;
        self.drop_column(table_id, column_id)
    }

    /// Execute `ALTER TABLE ... DROP COLUMN IF EXISTS`. Unresolved targets are
    /// suppressed only after current database-administrator authority is checked.
    pub fn execute_drop_column_if_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminTableColumnResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_column(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        match self.resolve_drop_column_sql_target(
            &statement.schema,
            &statement.table,
            &statement.column,
        ) {
            Ok((table_id, column_id)) => self.drop_column(table_id, column_id).map(Some),
            Err(ClientSubmitError::InvalidAdministrativeRequest) => {
                self.list_runtime_invocations_page(1, None)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn resolve_drop_column_sql_target(
        &mut self,
        schema: &str,
        table: &str,
        column: &str,
    ) -> Result<([u8; 16], [u8; 16]), ClientSubmitError> {
        let schema_id = self.resolve_schema_sql_name(schema)?;
        let table_id = self.resolve_table_sql_name_in_schema(schema_id, table)?;
        let mut cursor = None;
        loop {
            let page = self.list_columns_page(
                schema_id,
                table_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            if let Some(found) = page.records.iter().find(|record| record.name == column) {
                return Ok((table_id, found.column_id));
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                return Err(ClientSubmitError::InvalidAdministrativeRequest);
            }
        }
    }

    pub fn execute_rename_table_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_rename_table(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let table_id = self.resolve_table_sql_name(&statement.schema, &statement.table)?;
        self.rename_table(table_id, &statement.new_name)
    }

    pub fn execute_table_owner_transfer_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_table_owner_transfer(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let table = statement
            .table
            .as_deref()
            .ok_or(ClientSubmitError::InvalidAdministrativeRequest)?;
        let table_id = self.resolve_table_sql_name(&statement.schema, table)?;
        let kind = match statement.owner_kind {
            crate::ddl_sql::SqlIdentityKind::User => {
                crate::metadata_discovery::AdministrativeIdentityKind::User
            }
            crate::ddl_sql::SqlIdentityKind::Role => {
                crate::metadata_discovery::AdministrativeIdentityKind::Role
            }
            crate::ddl_sql::SqlIdentityKind::Either => {
                crate::metadata_discovery::AdministrativeIdentityKind::Either
            }
        };
        let owner_id =
            self.resolve_alter_identity_sql_name(table_id, &statement.owner_name, kind)?;
        self.set_table_owner(table_id, owner_id)
    }

    pub fn execute_drop_table_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_table(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let table_id = self.resolve_table_sql_name(&statement.schema, &statement.table)?;
        self.drop_table(table_id)
    }

    pub fn execute_truncate_table_sql(
        &mut self,
        sql: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_truncate_table(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let table_id = self.resolve_table_sql_name(&statement.schema, &statement.table)?;
        self.truncate_table(table_id)
    }

    /// Execute `DROP TABLE IF EXISTS`. If metadata cannot resolve the target,
    /// suppress absence only after the server confirms database-administrator
    /// authority; present tables still use the ordinary authorized drop path.
    pub fn execute_drop_table_if_exists_sql(
        &mut self,
        sql: &str,
    ) -> Result<Option<AdminOperationResult>, ClientSubmitError> {
        let statement = crate::ddl_sql::parse_drop_table(sql)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if !statement.if_exists {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        match self.resolve_table_sql_name(&statement.schema, &statement.table) {
            Ok(table_id) => self.drop_table(table_id).map(Some),
            Err(ClientSubmitError::InvalidAdministrativeRequest) => {
                self.list_runtime_invocations_page(1, None)?;
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn resolve_schema_sql_name(
        &mut self,
        name: &str,
    ) -> Result<[u8; 16], ClientSubmitError> {
        let mut cursor = None;
        let mut schema_id = None;
        loop {
            let page = self.list_schemas_page(DEFAULT_METADATA_PAGE_SIZE, cursor.as_deref())?;
            for visible in page.records {
                if visible.name == name && schema_id.replace(visible.schema_id).is_some() {
                    return Err(ClientSubmitError::InvalidResponse);
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        schema_id.ok_or(ClientSubmitError::InvalidAdministrativeRequest)
    }

    pub(super) fn resolve_table_sql_name(
        &mut self,
        schema: &str,
        table: &str,
    ) -> Result<[u8; 16], ClientSubmitError> {
        let schema_id = self.resolve_schema_sql_name(schema)?;
        self.resolve_table_sql_name_in_schema(schema_id, table)
    }

    pub(super) fn resolve_table_sql_name_in_schema(
        &mut self,
        schema_id: [u8; 16],
        table: &str,
    ) -> Result<[u8; 16], ClientSubmitError> {
        let mut table_id = None;
        let mut cursor = None;
        loop {
            let page =
                self.list_relations_page(schema_id, DEFAULT_METADATA_PAGE_SIZE, cursor.as_deref())?;
            for relation in page.records {
                if relation.name == table && table_id.replace(relation.relation_id).is_some() {
                    return Err(ClientSubmitError::InvalidResponse);
                }
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        table_id.ok_or(ClientSubmitError::InvalidAdministrativeRequest)
    }
}
