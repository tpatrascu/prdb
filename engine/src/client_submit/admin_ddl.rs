use super::codec::*;
use super::response::*;
use super::*;
use std::collections::HashSet;

impl AdminClient {
    pub fn create_schema(
        &mut self,
        name: &str,
        owner_id: [u8; 16],
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        self.create_schema_with_owner(name, Some(owner_id))
    }

    pub(super) fn create_schema_with_owner(
        &mut self,
        name: &str,
        owner_id: Option<[u8; 16]>,
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        validate_admin_name(name)?;
        if let Some(owner_id) = owner_id {
            validate_admin_id(owner_id)?;
        }
        let arguments = match owner_id {
            Some(owner_id) => format!(
                "{{\"name\":\"{name}\",\"owner_id\":\"{}\"}}",
                lower_hex(&owner_id)
            ),
            None => format!("{{\"name\":\"{name}\"}}"),
        };
        let (result, schema_id) =
            self.execute_admin_operation("schemas.create", arguments, Some("schema_id"))?;
        Ok(AdminIdentityResult {
            operation_id: result.operation_id,
            identity_id: schema_id.ok_or(ClientSubmitError::InvalidResponse)?,
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn drop_schema(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(schema_id)?;
        self.execute_admin_operation(
            "schemas.drop",
            format!("{{\"schema_id\":\"{}\"}}", lower_hex(&schema_id)),
            Some("schema_id"),
        )
        .map(|(result, _)| result)
    }

    pub fn rename_schema(
        &mut self,
        schema_id: [u8; 16],
        name: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(schema_id)?;
        validate_admin_name(name)?;
        self.execute_admin_operation_with_name_echo(
            "schemas.rename",
            format!(
                "{{\"schema_id\":\"{}\",\"name\":\"{name}\"}}",
                lower_hex(&schema_id)
            ),
            Some("schema_id"),
            Some(("name", name)),
        )
        .map(|(result, _)| result)
    }

    pub fn set_schema_owner(
        &mut self,
        schema_id: [u8; 16],
        owner_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(schema_id)?;
        validate_admin_id(owner_id)?;
        self.execute_admin_operation_with_fields(
            "schemas.set_owner",
            format!(
                "{{\"schema_id\":\"{}\",\"owner_id\":\"{}\"}}",
                lower_hex(&schema_id),
                lower_hex(&owner_id)
            ),
            None,
            &[
                AdminExpectedResponseField::Id("schema_id", schema_id),
                AdminExpectedResponseField::Id("owner_id", owner_id),
            ],
        )
        .map(|(result, _)| result)
    }

    pub fn rename_table(
        &mut self,
        table_id: [u8; 16],
        name: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_name(name)?;
        self.execute_admin_operation(
            "tables.rename",
            format!(
                "{{\"table_id\":\"{}\",\"name\":\"{name}\"}}",
                lower_hex(&table_id)
            ),
            Some("table_id"),
        )
        .map(|(result, _)| result)
    }

    pub fn drop_table(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        self.execute_admin_operation(
            "tables.drop",
            format!("{{\"table_id\":\"{}\"}}", lower_hex(&table_id)),
            Some("table_id"),
        )
        .map(|(result, _)| result)
    }

    pub fn truncate_table(
        &mut self,
        table_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        self.execute_admin_operation(
            "tables.truncate",
            format!("{{\"table_id\":\"{}\"}}", lower_hex(&table_id)),
            Some("table_id"),
        )
        .map(|(result, _)| result)
    }

    /// Create a table with typed columns. The result preserves the server's
    /// column order and returns each generated column ID in that same order.
    pub fn create_table(
        &mut self,
        schema_id: [u8; 16],
        name: &str,
        columns: &[AdminColumnDefinition],
    ) -> Result<AdminTableResult, ClientSubmitError> {
        self.create_table_internal(
            schema_id,
            name,
            columns,
            None,
            None,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
        )
    }

    pub fn create_table_with_primary_key(
        &mut self,
        schema_id: [u8; 16],
        name: &str,
        columns: &[AdminColumnDefinition],
        primary_key_columns: &[String],
    ) -> Result<AdminTableResult, ClientSubmitError> {
        self.create_table_internal(
            schema_id,
            name,
            columns,
            Some(primary_key_columns),
            None,
            &[],
            &[],
            &[],
            &[],
            &[],
            &[],
        )
    }

    pub fn create_table_with_constraints(
        &mut self,
        schema_id: [u8; 16],
        name: &str,
        columns: &[AdminColumnDefinition],
        primary_key_columns: Option<&[String]>,
        unique_constraints: &[Vec<String>],
    ) -> Result<AdminTableResult, ClientSubmitError> {
        self.create_table_internal(
            schema_id,
            name,
            columns,
            primary_key_columns,
            None,
            unique_constraints,
            &[],
            &[],
            &[],
            &[],
            &[],
        )
    }

    pub fn create_table_with_checks_and_constraints(
        &mut self,
        schema_id: [u8; 16],
        name: &str,
        columns: &[AdminColumnDefinition],
        primary_key_columns: Option<&[String]>,
        unique_constraints: &[Vec<String>],
        check_constraints: &[crate::authorization::Expr],
    ) -> Result<AdminTableResult, ClientSubmitError> {
        self.create_table_internal(
            schema_id,
            name,
            columns,
            primary_key_columns,
            None,
            unique_constraints,
            &[],
            check_constraints,
            &[],
            &[],
            &[],
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create_table_with_checks_constraints_and_foreign_keys(
        &mut self,
        schema_id: [u8; 16],
        name: &str,
        columns: &[AdminColumnDefinition],
        primary_key_columns: Option<&[String]>,
        unique_constraints: &[Vec<String>],
        check_constraints: &[crate::authorization::Expr],
        foreign_keys: &[AdminTableForeignKeyDefinition],
    ) -> Result<AdminTableResult, ClientSubmitError> {
        self.create_table_with_named_constraints_and_foreign_keys(
            schema_id,
            name,
            columns,
            primary_key_columns,
            None,
            unique_constraints,
            &[],
            check_constraints,
            &[],
            foreign_keys,
            &[],
        )
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn create_table_with_named_constraints_and_foreign_keys(
        &mut self,
        schema_id: [u8; 16],
        name: &str,
        columns: &[AdminColumnDefinition],
        primary_key_columns: Option<&[String]>,
        primary_key_name: Option<String>,
        unique_constraints: &[Vec<String>],
        unique_constraint_names: &[Option<String>],
        check_constraints: &[crate::authorization::Expr],
        check_constraint_names: &[Option<String>],
        foreign_keys: &[AdminTableForeignKeyDefinition],
        foreign_key_names: &[Option<String>],
    ) -> Result<AdminTableResult, ClientSubmitError> {
        self.create_table_internal(
            schema_id,
            name,
            columns,
            primary_key_columns,
            primary_key_name,
            unique_constraints,
            unique_constraint_names,
            check_constraints,
            check_constraint_names,
            foreign_keys,
            foreign_key_names,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn create_table_internal(
        &mut self,
        schema_id: [u8; 16],
        name: &str,
        columns: &[AdminColumnDefinition],
        primary_key_columns: Option<&[String]>,
        primary_key_name: Option<String>,
        unique_constraints: &[Vec<String>],
        unique_constraint_names: &[Option<String>],
        check_constraints: &[crate::authorization::Expr],
        check_constraint_names: &[Option<String>],
        foreign_keys: &[AdminTableForeignKeyDefinition],
        foreign_key_names: &[Option<String>],
    ) -> Result<AdminTableResult, ClientSubmitError> {
        validate_admin_id(schema_id)?;
        validate_admin_name(name)?;
        let encoded_columns = encode_admin_table_columns(columns)?;
        let primary_key_json = if let Some(primary_key_columns) = primary_key_columns {
            if primary_key_columns.is_empty()
                || primary_key_columns.len() > columns.len()
                || primary_key_columns.iter().collect::<HashSet<_>>().len()
                    != primary_key_columns.len()
                || primary_key_columns.iter().any(|name| {
                    !columns
                        .iter()
                        .any(|column| &column.name == name && !column.nullable)
                })
            {
                return Err(ClientSubmitError::InvalidAdministrativeRequest);
            }
            let names = primary_key_columns
                .iter()
                .map(|name| format!("\"{name}\""))
                .collect::<Vec<_>>()
                .join(",");
            format!(",\"primary_key_columns\":[{names}]")
        } else {
            String::new()
        };
        if unique_constraints.len() > crate::tuple::MAX_COLUMNS
            || unique_constraints.iter().any(|key_columns| {
                key_columns.is_empty()
                    || key_columns.len() > columns.len()
                    || key_columns.iter().collect::<HashSet<_>>().len() != key_columns.len()
                    || key_columns
                        .iter()
                        .any(|name| !columns.iter().any(|column| &column.name == name))
            })
            || unique_constraints
                .iter()
                .enumerate()
                .any(|(index, key_columns)| unique_constraints[..index].contains(key_columns))
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let unique_json = if unique_constraints.is_empty() {
            String::new()
        } else {
            let constraints = unique_constraints
                .iter()
                .map(|names| {
                    format!(
                        "[{}]",
                        names
                            .iter()
                            .map(|name| format!("\"{name}\""))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(",\"unique_constraints\":[{constraints}]")
        };
        if (primary_key_name.is_some() && primary_key_columns.is_none())
            || (!unique_constraint_names.is_empty()
                && unique_constraint_names.len() != unique_constraints.len())
            || unique_constraint_names
                .iter()
                .flatten()
                .chain(foreign_key_names.iter().flatten())
                .any(|name| !is_normalized_index_name(name))
            || (!foreign_key_names.is_empty() && foreign_key_names.len() != foreign_keys.len())
            || primary_key_name
                .as_ref()
                .is_some_and(|name| !is_normalized_index_name(name))
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let has_named_key_or_foreign_key = primary_key_name.is_some()
            || unique_constraint_names.iter().any(Option::is_some)
            || foreign_key_names.iter().any(Option::is_some);
        let named_key_json = if has_named_key_or_foreign_key {
            let primary_name = json_quote(primary_key_name.as_deref().unwrap_or_default());
            let unique_names = (0..unique_constraints.len())
                .map(|index| {
                    json_quote(
                        unique_constraint_names
                            .get(index)
                            .and_then(Option::as_deref)
                            .unwrap_or_default(),
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let foreign_names = (0..foreign_keys.len())
                .map(|index| {
                    json_quote(
                        foreign_key_names
                            .get(index)
                            .and_then(Option::as_deref)
                            .unwrap_or_default(),
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            format!(
                ",\"primary_key_name\":{primary_name},\"unique_constraint_names\":[{unique_names}],\"foreign_key_names\":[{foreign_names}]"
            )
        } else {
            String::new()
        };
        if check_constraints.len() > crate::tuple::MAX_COLUMNS
            || (!check_constraint_names.is_empty()
                && check_constraint_names.len() != check_constraints.len())
            || check_constraint_names
                .iter()
                .flatten()
                .any(|name| !is_normalized_index_name(name))
            || check_constraint_names
                .iter()
                .flatten()
                .collect::<HashSet<_>>()
                .len()
                != check_constraint_names.iter().flatten().count()
            || check_constraints.iter().any(|expression| {
                !crate::expression::is_row_local_expression(expression)
                    || crate::expression::encode_expression(expression).is_err()
            })
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let checks_json = if check_constraints.is_empty() {
            String::new()
        } else {
            let checks = check_constraints
                .iter()
                .map(|expression| {
                    crate::expression::encode_expression(expression)
                        .map(|bytes| format!("\"{}\"", lower_hex(&bytes)))
                        .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)
                })
                .collect::<Result<Vec<_>, _>>()?
                .join(",");
            format!(",\"check_expressions_eir1\":[{checks}]")
        };
        let check_names_json = if check_constraint_names.is_empty() {
            String::new()
        } else {
            let names = check_constraint_names
                .iter()
                .map(|name| json_quote(name.as_deref().unwrap_or_default()))
                .collect::<Vec<_>>()
                .join(",");
            format!(",\"check_constraint_names\":[{names}]")
        };
        if foreign_keys.len() > crate::tuple::MAX_COLUMNS
            || foreign_keys.iter().any(|foreign_key| {
                foreign_key.source_columns.is_empty()
                    || foreign_key.source_columns.len() > columns.len()
                    || (foreign_key.target_constraint_id == [0; 16])
                        != foreign_key.target_columns.is_some()
                    || foreign_key
                        .target_columns
                        .as_ref()
                        .is_some_and(|target_columns| {
                            target_columns.is_empty()
                                || target_columns.len() > columns.len()
                                || target_columns.iter().collect::<HashSet<_>>().len()
                                    != target_columns.len()
                                || target_columns
                                    .iter()
                                    .any(|name| !columns.iter().any(|column| &column.name == name))
                        })
                    || foreign_key
                        .source_columns
                        .iter()
                        .collect::<HashSet<_>>()
                        .len()
                        != foreign_key.source_columns.len()
                    || foreign_key.source_columns.iter().any(|column_name| {
                        !columns.iter().any(|column| &column.name == column_name)
                    })
            })
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let foreign_keys_json = if foreign_keys.is_empty() {
            String::new()
        } else {
            let source_columns = foreign_keys
                .iter()
                .map(|foreign_key| {
                    format!(
                        "[{}]",
                        foreign_key
                            .source_columns
                            .iter()
                            .map(|column| json_quote(column))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            let target_constraints = foreign_keys
                .iter()
                .map(|foreign_key| format!("\"{}\"", lower_hex(&foreign_key.target_constraint_id)))
                .collect::<Vec<_>>()
                .join(",");
            let target_columns = foreign_keys
                .iter()
                .map(|foreign_key| {
                    format!(
                        "[{}]",
                        foreign_key
                            .target_columns
                            .as_deref()
                            .unwrap_or_default()
                            .iter()
                            .map(|column| json_quote(column))
                            .collect::<Vec<_>>()
                            .join(",")
                    )
                })
                .collect::<Vec<_>>()
                .join(",");
            if foreign_keys
                .iter()
                .any(|foreign_key| foreign_key.target_columns.is_some())
            {
                format!(
                    ",\"foreign_key_source_columns\":[{source_columns}],\"foreign_key_target_constraint_ids\":[{target_constraints}],\"foreign_key_target_columns\":[{target_columns}]"
                )
            } else {
                format!(
                    ",\"foreign_key_source_columns\":[{source_columns}],\"foreign_key_target_constraint_ids\":[{target_constraints}]"
                )
            }
        };
        let operation_version = if has_named_key_or_foreign_key {
            4
        } else if !check_constraint_names.is_empty() {
            3
        } else if foreign_keys
            .iter()
            .any(|foreign_key| foreign_key.target_columns.is_some())
        {
            2
        } else {
            1
        };
        let prepare = format!(
            "{{\"phase\":\"prepare\",\"operation_version\":{operation_version},\"operation\":\"tables.create\",\"arguments\":{{\"schema_id\":\"{}\",\"name\":\"{name}\",\"columns\":{encoded_columns}{primary_key_json}{unique_json}{checks_json}{check_names_json}{named_key_json}{foreign_keys_json}}}}}",
            lower_hex(&schema_id)
        );
        let prepared = self.transport.exchange(10, prepare.into_bytes())?;
        let fields = parse_flat_response(&prepared.payload)?;
        let operation_id = response_id(&fields, "operation_id")?;
        if fields.len() != 2 || take_string(&fields, "state").as_deref() != Some("prepared") {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let start = format!(
            "{{\"phase\":\"start\",\"operation_id\":\"{}\"}}",
            lower_hex(&operation_id)
        );
        let started = match self.transport.exchange(10, start.into_bytes()) {
            Ok(response) => response,
            Err(ClientSubmitError::ServerRejected(message)) => {
                return Err(ClientSubmitError::AdministrativeOperationRejected {
                    operation_id,
                    message,
                });
            }
            Err(_) => {
                return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                    operation_id,
                ));
            }
        };
        let fields = parse_flat_response(&started.payload)
            .map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
        let response_operation_id = response_id(&fields, "operation_id").ok();
        let table_id = response_id(&fields, "table_id").ok();
        let outcome = fields.get("outcome").and_then(ResponseValue::as_string);
        let sequence = fields
            .get("commit_sequence")
            .and_then(ResponseValue::as_string)
            .and_then(|text| parse_canonical_positive_u64(text.to_owned()));
        let column_ids = fields.get("column_ids").and_then(|value| {
            let values = value.as_array()?;
            values
                .iter()
                .map(|value| {
                    value
                        .as_string()
                        .and_then(decode_hex_array::<16>)
                        .filter(|id| *id != [0; 16])
                })
                .collect::<Option<Vec<_>>>()
        });
        let valid_columns = column_ids.as_ref().is_some_and(|ids| {
            ids.len() == columns.len()
                && ids.iter().copied().collect::<HashSet<_>>().len() == ids.len()
        });
        if fields.len() != 5
            || response_operation_id != Some(operation_id)
            || table_id.is_none()
            || outcome != Some("committed")
            || sequence.is_none()
            || !valid_columns
        {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ));
        }
        Ok(AdminTableResult {
            operation_id,
            table_id: table_id.ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?,
            column_ids: column_ids.ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?,
            commit_sequence: sequence.ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?,
        })
    }

    pub fn add_column(
        &mut self,
        table_id: [u8; 16],
        column: &AdminColumnDefinition,
    ) -> Result<AdminTableColumnResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        if column.generated_uuid_identity
            || matches!(column.default, Some(AdminColumnDefault::CurrentPrincipalId))
            || (!column.nullable && column.default.is_none())
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let encoded = encode_admin_table_columns(std::slice::from_ref(column))?;
        let column_json = &encoded[1..encoded.len() - 1];
        let arguments = format!(
            "{{\"table_id\":\"{}\",\"column\":{column_json}}}",
            lower_hex(&table_id)
        );
        let (result, column_id) = self.execute_admin_operation_with_fields(
            "tables.add_column",
            arguments,
            Some("column_id"),
            &[
                AdminExpectedResponseField::Id("table_id", table_id),
                AdminExpectedResponseField::PositiveInteger("schema_version"),
            ],
        )?;
        Ok(AdminTableColumnResult {
            operation_id: result.operation_id,
            table_id,
            column_id: column_id.ok_or(ClientSubmitError::InvalidResponse)?,
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn drop_column(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
    ) -> Result<AdminTableColumnResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id(column_id)?;
        let arguments = format!(
            "{{\"table_id\":\"{}\",\"column_id\":\"{}\"}}",
            lower_hex(&table_id),
            lower_hex(&column_id)
        );
        let (result, response_column_id) = self.execute_admin_operation_with_fields(
            "tables.drop_column",
            arguments,
            Some("column_id"),
            &[AdminExpectedResponseField::Id("table_id", table_id)],
        )?;
        if response_column_id != Some(column_id) {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                result.operation_id,
            ));
        }
        Ok(AdminTableColumnResult {
            operation_id: result.operation_id,
            table_id,
            column_id,
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn rename_column(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
        name: &str,
    ) -> Result<AdminTableColumnResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id(column_id)?;
        validate_admin_name(name)?;
        let arguments = format!(
            "{{\"table_id\":\"{}\",\"column_id\":\"{}\",\"name\":\"{name}\"}}",
            lower_hex(&table_id),
            lower_hex(&column_id)
        );
        let (result, response_column_id) = self.execute_admin_operation_with_fields(
            "tables.rename_column",
            arguments,
            Some("column_id"),
            &[
                AdminExpectedResponseField::Id("table_id", table_id),
                AdminExpectedResponseField::String("name", name),
            ],
        )?;
        if response_column_id != Some(column_id) {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                result.operation_id,
            ));
        }
        Ok(AdminTableColumnResult {
            operation_id: result.operation_id,
            table_id,
            column_id,
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn set_table_owner(
        &mut self,
        table_id: [u8; 16],
        owner_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id(owner_id)?;
        self.execute_admin_operation_with_fields(
            "tables.set_owner",
            format!(
                "{{\"table_id\":\"{}\",\"owner_id\":\"{}\"}}",
                lower_hex(&table_id),
                lower_hex(&owner_id)
            ),
            None,
            &[
                AdminExpectedResponseField::Id("table_id", table_id),
                AdminExpectedResponseField::Id("owner_id", owner_id),
            ],
        )
        .map(|(result, _)| result)
    }

    pub fn alter_column_nullability(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
        nullable: bool,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id(column_id)?;
        self.execute_admin_operation_with_fields(
            "tables.alter_column_nullability",
            format!(
                "{{\"table_id\":\"{}\",\"column_id\":\"{}\",\"nullable\":\"{nullable}\"}}",
                lower_hex(&table_id),
                lower_hex(&column_id)
            ),
            None,
            &[
                AdminExpectedResponseField::Id("table_id", table_id),
                AdminExpectedResponseField::Id("column_id", column_id),
                AdminExpectedResponseField::Boolean("nullable", nullable),
            ],
        )
        .map(|(result, _)| result)
    }

    pub fn widen_varchar_column(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
        max_chars: u32,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id(column_id)?;
        if max_chars == 0 {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        self.execute_admin_operation_with_fields(
            "tables.widen_varchar_column",
            format!(
                "{{\"table_id\":\"{}\",\"column_id\":\"{}\",\"max_chars\":\"{max_chars}\"}}",
                lower_hex(&table_id),
                lower_hex(&column_id),
            ),
            None,
            &[
                AdminExpectedResponseField::Id("table_id", table_id),
                AdminExpectedResponseField::Id("column_id", column_id),
                AdminExpectedResponseField::String("max_chars", &max_chars.to_string()),
            ],
        )
        .map(|(result, _)| result)
    }

    pub fn set_column_default(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
        scalar_type: crate::tuple::ScalarType,
        default: AdminColumnDefault,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.alter_column_default(table_id, column_id, scalar_type, Some(default))
    }

    pub fn drop_column_default(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.alter_column_default(table_id, column_id, crate::tuple::ScalarType::Boolean, None)
    }

    pub(super) fn alter_column_default(
        &mut self,
        table_id: [u8; 16],
        column_id: [u8; 16],
        scalar_type: crate::tuple::ScalarType,
        default: Option<AdminColumnDefault>,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id(column_id)?;
        let (operation, arguments) = if let Some(default) = default {
            let scalar_type_name = encode_admin_scalar_type(scalar_type)?;
            let encoded_default = encode_admin_default_json(scalar_type, default)?;
            (
                "tables.set_column_default",
                format!(
                    "{{\"table_id\":\"{}\",\"column_id\":\"{}\",\"type\":{},{} }}",
                    lower_hex(&table_id),
                    lower_hex(&column_id),
                    json_quote(&scalar_type_name),
                    encoded_default
                ),
            )
        } else {
            (
                "tables.drop_column_default",
                format!(
                    "{{\"table_id\":\"{}\",\"column_id\":\"{}\"}}",
                    lower_hex(&table_id),
                    lower_hex(&column_id)
                ),
            )
        };
        self.execute_admin_operation_with_fields(
            operation,
            arguments,
            None,
            &[
                AdminExpectedResponseField::Id("table_id", table_id),
                AdminExpectedResponseField::Id("column_id", column_id),
            ],
        )
        .map(|(result, _)| result)
    }

    pub fn create_unique_constraint(
        &mut self,
        table_id: [u8; 16],
        column_ids: &[[u8; 16]],
    ) -> Result<AdminKeyConstraintResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id_list(column_ids, crate::tuple::MAX_COLUMNS)?;
        let (result, generated_ids) = self.execute_admin_operation_with_generated_ids(
            "constraints.create_unique",
            format!(
                "{{\"table_id\":\"{}\",\"column_ids\":{}}}",
                lower_hex(&table_id),
                encode_id_array(column_ids)
            ),
            &["constraint_id", "index_id"],
            &[],
        )?;
        Ok(AdminKeyConstraintResult {
            operation_id: result.operation_id,
            constraint_id: generated_ids[0],
            index_id: generated_ids[1],
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn create_primary_key(
        &mut self,
        table_id: [u8; 16],
        column_ids: &[[u8; 16]],
    ) -> Result<AdminKeyConstraintResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id_list(column_ids, crate::tuple::MAX_COLUMNS)?;
        let (result, generated_ids) = self.execute_admin_operation_with_generated_ids(
            "constraints.create_primary_key",
            format!(
                "{{\"table_id\":\"{}\",\"column_ids\":{}}}",
                lower_hex(&table_id),
                encode_id_array(column_ids)
            ),
            &["constraint_id", "index_id"],
            &[],
        )?;
        Ok(AdminKeyConstraintResult {
            operation_id: result.operation_id,
            constraint_id: generated_ids[0],
            index_id: generated_ids[1],
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn create_named_unique_constraint(
        &mut self,
        table_id: [u8; 16],
        column_ids: &[[u8; 16]],
        name: &str,
    ) -> Result<AdminKeyConstraintResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id_list(column_ids, crate::tuple::MAX_COLUMNS)?;
        validate_admin_name(name)?;
        let (result, generated_ids) = self.execute_admin_operation_with_generated_ids_version(
            2,
            "constraints.create_unique",
            format!(
                "{{\"table_id\":\"{}\",\"column_ids\":{},\"name\":{}}}",
                lower_hex(&table_id),
                encode_id_array(column_ids),
                json_quote(name),
            ),
            &["constraint_id", "index_id"],
            &[],
        )?;
        Ok(AdminKeyConstraintResult {
            operation_id: result.operation_id,
            constraint_id: generated_ids[0],
            index_id: generated_ids[1],
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn create_named_primary_key(
        &mut self,
        table_id: [u8; 16],
        column_ids: &[[u8; 16]],
        name: &str,
    ) -> Result<AdminKeyConstraintResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id_list(column_ids, crate::tuple::MAX_COLUMNS)?;
        validate_admin_name(name)?;
        let (result, generated_ids) = self.execute_admin_operation_with_generated_ids_version(
            2,
            "constraints.create_primary_key",
            format!(
                "{{\"table_id\":\"{}\",\"column_ids\":{},\"name\":{}}}",
                lower_hex(&table_id),
                encode_id_array(column_ids),
                json_quote(name),
            ),
            &["constraint_id", "index_id"],
            &[],
        )?;
        Ok(AdminKeyConstraintResult {
            operation_id: result.operation_id,
            constraint_id: generated_ids[0],
            index_id: generated_ids[1],
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn create_named_foreign_key(
        &mut self,
        source_table_id: [u8; 16],
        source_column_ids: &[[u8; 16]],
        target_constraint_id: [u8; 16],
        name: &str,
    ) -> Result<AdminForeignKeyResult, ClientSubmitError> {
        validate_admin_id(source_table_id)?;
        validate_admin_id(target_constraint_id)?;
        validate_admin_id_list(source_column_ids, crate::tuple::MAX_COLUMNS)?;
        validate_admin_name(name)?;
        let (result, generated_ids) = self.execute_admin_operation_with_generated_ids_version(
            2,
            "foreign_keys.create",
            format!(
                "{{\"source_table_id\":\"{}\",\"source_column_ids\":{},\"target_constraint_id\":\"{}\",\"name\":{}}}",
                lower_hex(&source_table_id),
                encode_id_array(source_column_ids),
                lower_hex(&target_constraint_id),
                json_quote(name),
            ),
            &["foreign_key_id"],
            &[],
        )?;
        Ok(AdminForeignKeyResult {
            operation_id: result.operation_id,
            foreign_key_id: generated_ids[0],
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn drop_primary_key(
        &mut self,
        constraint_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.drop_constraint("constraints.drop_primary_key", constraint_id)
    }

    pub fn drop_unique_constraint(
        &mut self,
        constraint_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.drop_constraint("constraints.drop_unique", constraint_id)
    }

    pub fn drop_check_constraint(
        &mut self,
        constraint_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        self.drop_constraint("constraints.drop_check", constraint_id)
    }

    pub(super) fn drop_constraint(
        &mut self,
        operation: &str,
        constraint_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(constraint_id)?;
        let (result, response_id) = self.execute_admin_operation(
            operation,
            format!("{{\"constraint_id\":\"{}\"}}", lower_hex(&constraint_id)),
            Some("constraint_id"),
        )?;
        if response_id != Some(constraint_id) {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                result.operation_id,
            ));
        }
        Ok(result)
    }

    pub fn create_check_constraint(
        &mut self,
        table_id: [u8; 16],
        expression: &crate::authorization::Expr,
        dependency_column_ids: &[[u8; 16]],
    ) -> Result<AdminCheckConstraintResult, ClientSubmitError> {
        self.create_check_constraint_inner(table_id, None, expression, dependency_column_ids)
    }

    pub fn create_named_check_constraint(
        &mut self,
        table_id: [u8; 16],
        name: &str,
        expression: &crate::authorization::Expr,
        dependency_column_ids: &[[u8; 16]],
    ) -> Result<AdminCheckConstraintResult, ClientSubmitError> {
        validate_admin_name(name)?;
        self.create_check_constraint_inner(table_id, Some(name), expression, dependency_column_ids)
    }

    pub(super) fn create_check_constraint_inner(
        &mut self,
        table_id: [u8; 16],
        name: Option<&str>,
        expression: &crate::authorization::Expr,
        dependency_column_ids: &[[u8; 16]],
    ) -> Result<AdminCheckConstraintResult, ClientSubmitError> {
        validate_admin_id(table_id)?;
        validate_admin_id_list_allow_empty(dependency_column_ids, crate::tuple::MAX_COLUMNS)?;
        if !crate::expression::is_row_local_expression(expression) {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let expression_bytes = crate::expression::encode_expression(expression)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        let arguments = if let Some(name) = name {
            format!(
                "{{\"table_id\":\"{}\",\"name\":{},\"expression_eir1\":\"{}\",\"dependency_column_ids\":{}}}",
                lower_hex(&table_id),
                json_quote(name),
                lower_hex(&expression_bytes),
                encode_id_array(dependency_column_ids)
            )
        } else {
            format!(
                "{{\"table_id\":\"{}\",\"expression_eir1\":\"{}\",\"dependency_column_ids\":{}}}",
                lower_hex(&table_id),
                lower_hex(&expression_bytes),
                encode_id_array(dependency_column_ids)
            )
        };
        let (result, generated_ids) = self.execute_admin_operation_with_generated_ids_version(
            if name.is_some() { 2 } else { 1 },
            "constraints.create_check",
            arguments,
            &["constraint_id", "expression_id"],
            &[],
        )?;
        Ok(AdminCheckConstraintResult {
            operation_id: result.operation_id,
            constraint_id: generated_ids[0],
            expression_id: generated_ids[1],
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn create_policy(
        &mut self,
        request: AdminCreatePolicyRequest<'_>,
    ) -> Result<AdminPolicyResult, ClientSubmitError> {
        self.create_or_replace_policy(request, None)
    }

    pub fn replace_policy(
        &mut self,
        request: AdminCreatePolicyRequest<'_>,
        replaced_policy_id: [u8; 16],
    ) -> Result<AdminPolicyResult, ClientSubmitError> {
        validate_admin_id(replaced_policy_id)?;
        self.create_or_replace_policy(request, Some(replaced_policy_id))
    }

    pub(super) fn create_or_replace_policy(
        &mut self,
        request: AdminCreatePolicyRequest<'_>,
        replaced_policy_id: Option<[u8; 16]>,
    ) -> Result<AdminPolicyResult, ClientSubmitError> {
        let AdminCreatePolicyRequest {
            table_id,
            name,
            kind,
            operation,
            effect,
            recipients,
            target_column_ids,
            using_expression,
            with_check_expression,
        } = request;
        validate_admin_id(table_id)?;
        validate_admin_name(name)?;
        validate_admin_id_list_allow_empty(target_column_ids, crate::tuple::MAX_COLUMNS)?;
        if recipients.is_empty()
            || recipients.len() > crate::authorization::MAX_POLICY_SUBJECTS
            || recipients
                .iter()
                .copied()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != recipients.len()
            || recipients.iter().any(|recipient| match recipient {
                AdminPolicyRecipient::User(id) | AdminPolicyRecipient::Role(id) => *id == [0; 16],
            })
            || (kind == AdminPolicyKind::Row && !target_column_ids.is_empty())
            || (kind == AdminPolicyKind::Cell && target_column_ids.is_empty())
            || (kind == AdminPolicyKind::Cell && operation == AdminPolicyOperation::Delete)
            || !match operation {
                AdminPolicyOperation::Select | AdminPolicyOperation::Delete => {
                    using_expression.is_some() && with_check_expression.is_none()
                }
                AdminPolicyOperation::Insert => {
                    using_expression.is_none() && with_check_expression.is_some()
                }
                AdminPolicyOperation::Update => {
                    using_expression.is_some() && with_check_expression.is_some()
                }
            }
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let encode_optional_expression = |expression: Option<&crate::authorization::Expr>| {
            expression
                .map(crate::expression::encode_expression)
                .transpose()
                .map(|bytes| bytes.map(|bytes| lower_hex(&bytes)))
                .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)
        };
        let using_ir = encode_optional_expression(using_expression)?;
        let check_ir = encode_optional_expression(with_check_expression)?;
        let recipients_json = recipients
            .iter()
            .map(|recipient| {
                let (kind, id) = match recipient {
                    AdminPolicyRecipient::User(id) => ("user", id),
                    AdminPolicyRecipient::Role(id) => ("role", id),
                };
                format!("\"{kind}:{}\"", lower_hex(id))
            })
            .collect::<Vec<_>>()
            .join(",");
        let using_json = using_ir
            .map(|value| format!("\"{value}\""))
            .unwrap_or_else(|| "null".to_owned());
        let check_json = check_ir
            .map(|value| format!("\"{value}\""))
            .unwrap_or_else(|| "null".to_owned());
        let operation_name = if replaced_policy_id.is_some() {
            "policies.replace"
        } else {
            "policies.create"
        };
        let replacement_argument = replaced_policy_id
            .map(|id| format!(",\"replaced_policy_id\":\"{}\"", lower_hex(&id)))
            .unwrap_or_default();
        let (result, generated_ids) = self.execute_admin_operation_with_generated_ids(
            operation_name,
            format!(
                "{{\"table_id\":\"{}\",\"name\":{},\"kind\":\"{}\",\"policy_operation\":\"{}\",\"effect\":\"{}\",\"recipients\":[{}],\"target_column_ids\":{},\"using_expression_eir1\":{},\"with_check_expression_eir1\":{}{replacement_argument}}}",
                lower_hex(&table_id),
                json_quote(name),
                kind.as_wire_name(),
                operation.as_wire_name(),
                effect.as_wire_name(),
                recipients_json,
                encode_id_array(target_column_ids),
                using_json,
                check_json,
            ),
            &["policy_id"],
            &[],
        )?;
        Ok(AdminPolicyResult {
            operation_id: result.operation_id,
            policy_id: generated_ids[0],
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn drop_policy(
        &mut self,
        policy_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(policy_id)?;
        let (result, response_id) = self.execute_admin_operation(
            "policies.drop",
            format!("{{\"policy_id\":\"{}\"}}", lower_hex(&policy_id)),
            Some("policy_id"),
        )?;
        if response_id != Some(policy_id) {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                result.operation_id,
            ));
        }
        Ok(result)
    }

    pub fn create_foreign_key(
        &mut self,
        source_table_id: [u8; 16],
        source_column_ids: &[[u8; 16]],
        target_constraint_id: [u8; 16],
    ) -> Result<AdminForeignKeyResult, ClientSubmitError> {
        validate_admin_id(source_table_id)?;
        validate_admin_id(target_constraint_id)?;
        validate_admin_id_list(source_column_ids, crate::tuple::MAX_COLUMNS)?;
        let (result, foreign_key_id) = self.execute_admin_operation(
            "foreign_keys.create",
            format!(
                "{{\"source_table_id\":\"{}\",\"source_column_ids\":{},\"target_constraint_id\":\"{}\"}}",
                lower_hex(&source_table_id),
                encode_id_array(source_column_ids),
                lower_hex(&target_constraint_id)
            ),
            Some("foreign_key_id"),
        )?;
        Ok(AdminForeignKeyResult {
            operation_id: result.operation_id,
            foreign_key_id: foreign_key_id.ok_or(ClientSubmitError::InvalidResponse)?,
            commit_sequence: result.commit_sequence,
        })
    }

    pub fn drop_foreign_key(
        &mut self,
        foreign_key_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(foreign_key_id)?;
        let (result, response_id) = self.execute_admin_operation(
            "foreign_keys.drop",
            format!("{{\"foreign_key_id\":\"{}\"}}", lower_hex(&foreign_key_id)),
            Some("foreign_key_id"),
        )?;
        if response_id != Some(foreign_key_id) {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                result.operation_id,
            ));
        }
        Ok(result)
    }

    pub(super) fn create_identity(
        &mut self,
        operation: &str,
        name: &str,
        id_field: &str,
    ) -> Result<AdminIdentityResult, ClientSubmitError> {
        validate_admin_name(name)?;
        let (result, identity_id) = self.execute_admin_operation(
            operation,
            format!("{{\"name\":\"{name}\"}}"),
            Some(id_field),
        )?;
        Ok(AdminIdentityResult {
            operation_id: result.operation_id,
            identity_id: identity_id.ok_or(ClientSubmitError::InvalidResponse)?,
            commit_sequence: result.commit_sequence,
        })
    }

    pub(super) fn rename_identity(
        &mut self,
        operation: &str,
        id_field: &str,
        identity_id: [u8; 16],
        name: &str,
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(identity_id)?;
        validate_admin_name(name)?;
        self.execute_admin_operation(
            operation,
            format!(
                "{{\"{id_field}\":\"{}\",\"name\":\"{name}\"}}",
                lower_hex(&identity_id)
            ),
            None,
        )
        .map(|(result, _)| result)
    }

    pub(super) fn drop_identity(
        &mut self,
        operation: &str,
        id_field: &str,
        identity_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(identity_id)?;
        self.execute_admin_operation(
            operation,
            format!("{{\"{id_field}\":\"{}\"}}", lower_hex(&identity_id)),
            None,
        )
        .map(|(result, _)| result)
    }

    pub(super) fn change_role_membership(
        &mut self,
        operation: &str,
        member_id: [u8; 16],
        role_id: [u8; 16],
    ) -> Result<AdminOperationResult, ClientSubmitError> {
        validate_admin_id(member_id)?;
        validate_admin_id(role_id)?;
        if member_id == role_id {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        self.execute_admin_operation(
            operation,
            format!(
                "{{\"member_id\":\"{}\",\"role_id\":\"{}\"}}",
                lower_hex(&member_id),
                lower_hex(&role_id)
            ),
            None,
        )
        .map(|(result, _)| result)
    }

    pub(super) fn execute_admin_operation(
        &mut self,
        operation: &str,
        arguments: String,
        created_id_field: Option<&str>,
    ) -> Result<(AdminOperationResult, Option<[u8; 16]>), ClientSubmitError> {
        self.execute_admin_operation_with_fields(operation, arguments, created_id_field, &[])
    }

    pub(super) fn execute_admin_operation_with_name_echo(
        &mut self,
        operation: &str,
        arguments: String,
        created_id_field: Option<&str>,
        expected_name: Option<(&str, &str)>,
    ) -> Result<(AdminOperationResult, Option<[u8; 16]>), ClientSubmitError> {
        let expected_fields = expected_name
            .map(|(key, value)| vec![AdminExpectedResponseField::String(key, value)])
            .unwrap_or_default();
        self.execute_admin_operation_with_fields(
            operation,
            arguments,
            created_id_field,
            &expected_fields,
        )
    }

    pub(super) fn execute_admin_operation_with_fields(
        &mut self,
        operation: &str,
        arguments: String,
        created_id_field: Option<&str>,
        expected_response_fields: &[AdminExpectedResponseField<'_>],
    ) -> Result<(AdminOperationResult, Option<[u8; 16]>), ClientSubmitError> {
        let generated_id_fields = created_id_field.into_iter().collect::<Vec<_>>();
        self.execute_admin_operation_with_generated_ids(
            operation,
            arguments,
            &generated_id_fields,
            expected_response_fields,
        )
        .map(|(result, ids)| (result, ids.first().copied()))
    }

    pub(super) fn execute_admin_operation_with_generated_ids(
        &mut self,
        operation: &str,
        arguments: String,
        generated_id_fields: &[&str],
        expected_response_fields: &[AdminExpectedResponseField<'_>],
    ) -> Result<(AdminOperationResult, Vec<[u8; 16]>), ClientSubmitError> {
        self.execute_admin_operation_with_generated_ids_version(
            1,
            operation,
            arguments,
            generated_id_fields,
            expected_response_fields,
        )
    }

    pub(super) fn execute_admin_operation_with_generated_ids_version(
        &mut self,
        operation_version: u32,
        operation: &str,
        arguments: String,
        generated_id_fields: &[&str],
        expected_response_fields: &[AdminExpectedResponseField<'_>],
    ) -> Result<(AdminOperationResult, Vec<[u8; 16]>), ClientSubmitError> {
        let prepare = format!(
            "{{\"phase\":\"prepare\",\"operation_version\":{operation_version},\"operation\":\"{operation}\",\"arguments\":{arguments}}}"
        );
        let prepared = self.transport.exchange(10, prepare.into_bytes())?;
        let fields = parse_flat_response(&prepared.payload)?;
        let operation_id = response_id(&fields, "operation_id")?;
        if fields.len() != 2 || take_string(&fields, "state").as_deref() != Some("prepared") {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let start = format!(
            "{{\"phase\":\"start\",\"operation_id\":\"{}\"}}",
            lower_hex(&operation_id)
        );
        let started = match self.transport.exchange(10, start.into_bytes()) {
            Ok(response) => response,
            Err(ClientSubmitError::ServerRejected(message)) => {
                return Err(ClientSubmitError::AdministrativeOperationRejected {
                    operation_id,
                    message,
                });
            }
            Err(_) => {
                return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                    operation_id,
                ));
            }
        };
        let fields = parse_flat_response(&started.payload)
            .map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
        let response_operation_id = response_id(&fields, "operation_id").ok();
        let outcome = fields.get("outcome").and_then(ResponseValue::as_string);
        let sequence = fields
            .get("commit_sequence")
            .and_then(ResponseValue::as_string)
            .and_then(|text| parse_canonical_positive_u64(text.to_owned()));
        let generated_ids = generated_id_fields
            .iter()
            .map(|key| response_id(&fields, key).ok())
            .collect::<Option<Vec<_>>>();
        let expected_field_count = 3 + generated_id_fields.len() + expected_response_fields.len();
        if fields.len() != expected_field_count
            || response_operation_id != Some(operation_id)
            || outcome != Some("committed")
            || sequence.is_none()
            || generated_ids.is_none()
            || expected_response_fields
                .iter()
                .any(|expectation| !expectation.matches(&fields))
        {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ));
        }
        Ok((
            AdminOperationResult {
                operation_id,
                commit_sequence: sequence.unwrap(),
            },
            generated_ids.unwrap(),
        ))
    }

    /// Create an ordinary index using the authenticated control connection.
    pub fn create_index(
        &mut self,
        table_id: [u8; 16],
        name: &str,
        column_ids: &[[u8; 16]],
        unique: bool,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        validate_index_request(table_id, column_ids)?;
        if !is_normalized_index_name(name) {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let columns = encode_id_array(column_ids);
        let arguments = format!(
            "{{\"table_id\":\"{}\",\"name\":\"{name}\",\"column_ids\":{columns},\"unique\":{unique}}}",
            lower_hex(&table_id)
        );
        self.execute_index_ddl("indexes.create", arguments, None)
    }

    /// Rebuild an index definition and its memberships through the control
    /// channel. An uncertain START result includes the server-issued
    /// operation ID for status reconciliation; START is never retried.
    pub fn rebuild_index(
        &mut self,
        index_id: [u8; 16],
        column_ids: &[[u8; 16]],
        unique: bool,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        validate_index_request(index_id, column_ids)?;
        let arguments = format!(
            "{{\"index_id\":\"{}\",\"column_ids\":{},\"unique\":{unique}}}",
            lower_hex(&index_id),
            encode_id_array(column_ids)
        );
        self.execute_index_ddl("indexes.rebuild", arguments, Some(index_id))
    }

    /// Drop an ordinary index through the authenticated control channel.
    pub fn drop_index(
        &mut self,
        index_id: [u8; 16],
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        if index_id == [0; 16] {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        self.execute_index_ddl(
            "indexes.drop",
            format!("{{\"index_id\":\"{}\"}}", lower_hex(&index_id)),
            Some(index_id),
        )
    }

    /// Rename an ordinary index through the authenticated control channel.
    pub fn rename_index(
        &mut self,
        index_id: [u8; 16],
        name: &str,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        if index_id == [0; 16] || !is_normalized_index_name(name) {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        self.execute_index_ddl(
            "indexes.rename",
            format!(
                "{{\"index_id\":\"{}\",\"name\":\"{name}\"}}",
                lower_hex(&index_id)
            ),
            Some(index_id),
        )
    }

    /// Reconcile a prior administrative operation by operation ID. Use this
    /// after reconnecting if START returned `AdministrativeOutcomeUncertain`.
    pub fn status(
        &mut self,
        operation_id: [u8; 16],
    ) -> Result<InvocationStatus, ClientSubmitError> {
        if operation_id == [0; 16] {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let payload = format!("{{\"invocation_id\":\"{}\"}}", lower_hex(&operation_id));
        let response = self.transport.exchange(8, payload.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 1 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        match take_string(&fields, "state").as_deref() {
            Some("prepared") => Ok(InvocationStatus::Prepared),
            Some("active") => Ok(InvocationStatus::Active),
            Some("committing") => Ok(InvocationStatus::Committing),
            Some("committed") => Ok(InvocationStatus::Committed),
            Some("aborted") => Ok(InvocationStatus::Aborted),
            Some("unknown") => Ok(InvocationStatus::Unknown),
            Some("expired") => Ok(InvocationStatus::Expired),
            _ => Err(ClientSubmitError::InvalidResponse),
        }
    }

    /// Request administrator-authorized cancellation through control mode.
    pub fn cancel_invocation(
        &mut self,
        invocation_id: [u8; 16],
    ) -> Result<CancellationResult, ClientSubmitError> {
        self.transport.cancel(invocation_id)
    }

    pub(super) fn execute_index_ddl(
        &mut self,
        operation: &str,
        arguments: String,
        expected_index_id: Option<[u8; 16]>,
    ) -> Result<AdminIndexResult, ClientSubmitError> {
        let prepare = format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"{operation}\",\"arguments\":{arguments}}}"
        );
        let prepared = self.transport.exchange(10, prepare.into_bytes())?;
        let fields = parse_flat_response(&prepared.payload)?;
        let operation_id = response_id(&fields, "operation_id")?;
        if fields.len() != 2 || take_string(&fields, "state").as_deref() != Some("prepared") {
            return Err(ClientSubmitError::InvalidResponse);
        }

        let start = format!(
            "{{\"phase\":\"start\",\"operation_id\":\"{}\"}}",
            lower_hex(&operation_id)
        );
        let started = match self.transport.exchange(10, start.into_bytes()) {
            Ok(response) => response,
            Err(ClientSubmitError::ServerRejected(message)) => {
                return Err(ClientSubmitError::AdministrativeOperationRejected {
                    operation_id,
                    message,
                });
            }
            Err(_) => {
                return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                    operation_id,
                ));
            }
        };
        let fields = parse_flat_response(&started.payload)
            .map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
        let response_operation_id = response_id(&fields, "operation_id").ok();
        let index_id = response_id(&fields, "index_id").ok();
        let outcome = take_string(&fields, "outcome");
        let sequence =
            take_string(&fields, "commit_sequence").and_then(parse_canonical_positive_u64);
        if fields.len() != 4
            || response_operation_id != Some(operation_id)
            || outcome.as_deref() != Some("committed")
            || index_id.is_none()
            || expected_index_id.is_some_and(|expected| index_id != Some(expected))
            || sequence.is_none()
        {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ));
        }
        Ok(AdminIndexResult {
            operation_id,
            index_id: index_id.ok_or(ClientSubmitError::InvalidResponse)?,
            commit_sequence: sequence.ok_or(ClientSubmitError::InvalidResponse)?,
        })
    }
}
