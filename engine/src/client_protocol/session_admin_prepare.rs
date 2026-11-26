use super::json::*;
use super::types::*;
use super::*;
use crate::transaction::TransactionError;

impl ClientConnectionSession {
    pub(super) fn handle_admin_prepare(
        &mut self,
        request: &ClientFrame,
        command: ClientAdminCommand,
    ) -> Result<ClientFrame, ClientSessionError> {
        match command {
            ClientAdminCommand::PrepareCreateUser {
                name,
                workload_profile,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateUser {
                        name,
                        workload_profile,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateUserNoLogin {
                name,
                workload_profile,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateUserNoLogin {
                        name,
                        workload_profile,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateRole { name } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateRole { name },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateSchema { name, owner_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateSchema { name, owner_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropSchema { schema_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropSchema { schema_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareSetSchemaOwner {
                schema_id,
                owner_id,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::SetSchemaOwner {
                        schema_id,
                        owner_id,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRenameSchema { schema_id, name } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::RenameSchema { schema_id, name },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropTable { table_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropTable { table_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareTruncateTable { table_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::TruncateTable { table_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropForeignKey { foreign_key_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropForeignKey { foreign_key_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropCheckConstraint { constraint_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropCheckConstraint { constraint_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropPrimaryKey { constraint_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropPrimaryKey { constraint_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropUniqueConstraint { constraint_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropUniqueConstraint { constraint_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateForeignKey {
                source_table_id,
                source_column_ids,
                target_constraint_id,
                name,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateForeignKey {
                        source_table_id,
                        source_column_ids,
                        target_constraint_id,
                        name,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateUniqueConstraint {
                table_id,
                column_ids,
                name,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateUniqueConstraint {
                        table_id,
                        column_ids,
                        name,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateIndex {
                table_id,
                name,
                column_ids,
                unique,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateIndex {
                        table_id,
                        name,
                        column_ids,
                        unique,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropIndex { index_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropIndex { index_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRebuildIndex {
                index_id,
                column_ids,
                unique,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::RebuildIndex {
                        index_id,
                        column_ids,
                        unique,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRenameIndex { index_id, name } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::RenameIndex { index_id, name },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreatePrimaryKey {
                table_id,
                column_ids,
                name,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreatePrimaryKey {
                        table_id,
                        column_ids,
                        name,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateCheckConstraint {
                table_id,
                name,
                expression,
                dependency_column_ids,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateCheckConstraint {
                        table_id,
                        name,
                        expression,
                        dependency_column_ids,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreatePolicy {
                replaced_policy_id,
                table_id,
                name,
                kind,
                operation,
                effect,
                recipients,
                target_column_ids,
                using_expression,
                with_check_expression,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreatePolicy {
                        replaced_policy_id,
                        table_id,
                        name,
                        kind,
                        operation,
                        effect,
                        recipients,
                        target_column_ids,
                        using_expression,
                        with_check_expression,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropPolicy { policy_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropPolicy { policy_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRenameTable { table_id, name } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::RenameTable { table_id, name },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareSetTableOwner { table_id, owner_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::SetTableOwner { table_id, owner_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRenameColumn {
                table_id,
                column_id,
                name,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::RenameColumn {
                        table_id,
                        column_id,
                        name,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareSetColumnNullable {
                table_id,
                column_id,
                nullable,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::SetColumnNullable {
                        table_id,
                        column_id,
                        nullable,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareWidenVarcharColumn {
                table_id,
                column_id,
                max_chars,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::WidenVarcharColumn {
                        table_id,
                        column_id,
                        max_chars,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareAlterColumnDefault {
                table_id,
                column_id,
                default,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::AlterColumnDefault {
                        table_id,
                        column_id,
                        default,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareAddColumn { table_id, column } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::AddColumn { table_id, column },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareDropColumn {
                table_id,
                column_id,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::DropColumn {
                        table_id,
                        column_id,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareCreateTable {
                schema_id,
                name,
                columns,
                primary_key_columns,
                primary_key_name,
                unique_constraints,
                unique_constraint_names,
                check_expressions,
                check_constraint_names,
                foreign_keys,
                foreign_key_names,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::CreateTable {
                        schema_id,
                        name,
                        columns,
                        primary_key_columns,
                        primary_key_name,
                        unique_constraints,
                        unique_constraint_names,
                        check_expressions,
                        check_constraint_names,
                        foreign_keys,
                        foreign_key_names,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRenameIdentity { identity_id, name } => {
                self.prepare_identity_rename(request.request_id, identity_id, name)
            }
            ClientAdminCommand::PrepareDropIdentity { identity_id } => {
                self.prepare_identity_drop(request.request_id, identity_id)
            }
            ClientAdminCommand::PrepareSetUserLogin { user_id, enabled } => {
                self.prepare_user_login(request.request_id, user_id, enabled)
            }
            ClientAdminCommand::PrepareSetUserWorkloadProfile {
                user_id,
                profile_name,
            } => self.prepare_simple_admin_action(
                request.request_id,
                PendingAdminAction::SetUserWorkloadProfile {
                    user_id,
                    profile_name,
                },
            ),
            ClientAdminCommand::PrepareCreateWorkloadProfile {
                name,
                base_profile,
                settings,
            } => self.prepare_simple_admin_action(
                request.request_id,
                PendingAdminAction::CreateWorkloadProfile {
                    name,
                    base_profile,
                    settings,
                },
            ),
            ClientAdminCommand::PrepareAlterWorkloadProfile { name, settings } => self
                .prepare_simple_admin_action(
                    request.request_id,
                    PendingAdminAction::AlterWorkloadProfile { name, settings },
                ),
            ClientAdminCommand::PrepareRenameWorkloadProfile { old_name, new_name } => self
                .prepare_simple_admin_action(
                    request.request_id,
                    PendingAdminAction::RenameWorkloadProfile { old_name, new_name },
                ),
            ClientAdminCommand::PrepareDropWorkloadProfile { name } => self
                .prepare_simple_admin_action(
                    request.request_id,
                    PendingAdminAction::DropWorkloadProfile { name },
                ),
            ClientAdminCommand::PrepareGrantRole { member_id, role_id } => {
                self.prepare_role_membership(request.request_id, member_id, role_id, true)
            }
            ClientAdminCommand::PrepareRevokeRole { member_id, role_id } => {
                self.prepare_role_membership(request.request_id, member_id, role_id, false)
            }
            ClientAdminCommand::PrepareSystemPrivilege {
                grantee_id,
                privilege,
                grant,
            } => self.prepare_system_privilege(request.request_id, grantee_id, privilege, grant),
            ClientAdminCommand::PrepareObjectGrant {
                grantee_id,
                object_id,
                privileges,
                target,
                entries,
                grant,
            } => self.prepare_object_grant(
                request.request_id,
                grantee_id,
                object_id,
                privileges,
                target,
                entries,
                grant,
            ),
            ClientAdminCommand::PrepareRevoke { token_id } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::Revoke { token_id },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareIssue {
                user_id,
                label,
                expires_at_us,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
                if expires_at_us <= now_us {
                    return Err(ClientSessionError::InvalidControlPayload(
                        ClientControlError::InvalidPrepare,
                    ));
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::Issue {
                        user_id,
                        label,
                        expires_at_us,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            ClientAdminCommand::PrepareRotateToken {
                old_token_id,
                label,
                expires_at_us,
            } => {
                if self.pending_admin_operation.is_some() {
                    return Err(ClientSessionError::WrongMode);
                }
                let now_us = current_unix_micros().ok_or(ClientSessionError::UnsupportedRequest)?;
                if expires_at_us <= now_us {
                    return Err(ClientSessionError::InvalidControlPayload(
                        ClientControlError::InvalidPrepare,
                    ));
                }
                let mut operation_id = [0; 16];
                getrandom::fill(&mut operation_id).map_err(|_| {
                    ClientSessionError::Storage(TransactionError::RandomnessUnavailable)
                })?;
                if operation_id == [0; 16] {
                    return Err(ClientSessionError::Storage(
                        TransactionError::RandomnessUnavailable,
                    ));
                }
                self.pending_admin_operation = Some(PendingAdminOperation {
                    operation_id,
                    action: PendingAdminAction::Rotate {
                        old_token_id,
                        label,
                        expires_at_us,
                    },
                });
                self.json_response(
                    request.request_id,
                    format!(
                        "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                        encode_lower_hex_id(operation_id)
                    )
                    .into_bytes(),
                )
            }
            _ => unreachable!("non-prepare command routed to handle_admin_prepare"),
        }
    }

    pub(super) fn prepare_role_membership(
        &mut self,
        request_id: u64,
        member_id: [u8; 16],
        role_id: [u8; 16],
        grant: bool,
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        let action = PendingAdminAction::ChangeRoleMembership {
            member_id,
            role_id,
            grant,
        };
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action,
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    pub(super) fn prepare_system_privilege(
        &mut self,
        request_id: u64,
        grantee_id: [u8; 16],
        privilege: crate::storage_manager::AdministrativeSystemPrivilege,
        grant: bool,
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action: PendingAdminAction::ChangeSystemPrivilege {
                grantee_id,
                privilege,
                grant,
            },
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    #[allow(clippy::too_many_arguments)] // Preserve the decoded ADMIN request fields without an intermediate allocation.
    pub(super) fn prepare_object_grant(
        &mut self,
        request_id: u64,
        grantee_id: [u8; 16],
        object_id: [u8; 16],
        privileges: Vec<crate::storage_manager::ObjectGrantPrivilege>,
        target: crate::storage_manager::ObjectGrantTarget,
        entries: Option<Vec<crate::storage_manager::ObjectGrantEntry>>,
        grant: bool,
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action: PendingAdminAction::ChangeObjectGrant {
                grantee_id,
                object_id,
                privileges,
                target,
                entries,
                grant,
            },
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    pub(super) fn prepare_identity_rename(
        &mut self,
        request_id: u64,
        identity_id: [u8; 16],
        name: String,
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action: PendingAdminAction::RenameIdentity { identity_id, name },
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    pub(super) fn prepare_simple_admin_action(
        &mut self,
        request_id: u64,
        action: PendingAdminAction,
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action,
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    pub(super) fn prepare_user_login(
        &mut self,
        request_id: u64,
        user_id: [u8; 16],
        enabled: bool,
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action: PendingAdminAction::SetUserLogin { user_id, enabled },
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }

    pub(super) fn prepare_identity_drop(
        &mut self,
        request_id: u64,
        identity_id: [u8; 16],
    ) -> Result<ClientFrame, ClientSessionError> {
        if self.pending_admin_operation.is_some() {
            return Err(ClientSessionError::WrongMode);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSessionError::Storage(TransactionError::RandomnessUnavailable))?;
        if operation_id == [0; 16] {
            return Err(ClientSessionError::Storage(
                TransactionError::RandomnessUnavailable,
            ));
        }
        self.pending_admin_operation = Some(PendingAdminOperation {
            operation_id,
            action: PendingAdminAction::DropIdentity { identity_id },
        });
        self.json_response(
            request_id,
            format!(
                "{{\"operation_id\":\"{}\",\"state\":\"prepared\"}}",
                encode_lower_hex_id(operation_id)
            )
            .into_bytes(),
        )
    }
}
