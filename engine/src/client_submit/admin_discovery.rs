use super::codec::*;
use super::metadata::*;
use super::response::*;
use super::*;
use crate::client_protocol::{CLIENT_MAX_CHUNK_BYTES, CLIENT_MAX_CONTROL_BYTES};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

impl AdminClient {
    /// Connect over TCP in control mode and authenticate. The caller's
    /// credential buffer is zeroized on every return path.
    pub fn connect_authenticated(
        address: SocketAddr,
        token_id: [u8; 16],
        token_secret: &mut [u8],
    ) -> Result<Self, ClientSubmitError> {
        Self::connect_authenticated_with_timeout(address, token_id, token_secret, DEFAULT_TIMEOUT)
    }

    pub fn connect_authenticated_with_timeout(
        address: SocketAddr,
        token_id: [u8; 16],
        token_secret: &mut [u8],
        timeout: Duration,
    ) -> Result<Self, ClientSubmitError> {
        let transport = SubmissionClient::connect_authenticated_mode(
            address,
            token_id,
            token_secret,
            timeout,
            crate::client_protocol::ClientMode::Control,
            1,
            false,
        )?;
        Ok(Self { transport })
    }

    /// List the SDK releases installed in the connected database instance.
    /// Listing requires authentication but not database-administrator role.
    pub fn list_sdk_libraries(&mut self) -> Result<Vec<SdkLibraryInfo>, ClientSubmitError> {
        let mut releases = Vec::new();
        let mut cursor = None;
        let mut previous_release = 0;
        loop {
            let page =
                self.list_sdk_libraries_page(DEFAULT_METADATA_PAGE_SIZE, cursor.as_deref())?;
            for release in &page.records {
                if release.sdk_release <= previous_release {
                    return Err(ClientSubmitError::InvalidResponse);
                }
                previous_release = release.sdk_release;
            }
            releases.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                return Ok(releases);
            }
        }
    }

    /// Return one ordered SDK release page. Continuations are opaque and
    /// session-bound to the authenticated control connection.
    pub fn list_sdk_libraries_page(
        &mut self,
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<SdkLibraryInfo>, ClientSubmitError> {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = metadata_page_arguments(page_limit, cursor);
        let request = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"sdk_libraries.list\",\"arguments\":{arguments}}}"
        );
        let response = self.transport.exchange(10, request.into_bytes())?;
        let root = parse_bounded_json_value(&response.payload, CLIENT_MAX_CONTROL_BYTES)?;
        let fields = root.as_object().ok_or(ClientSubmitError::InvalidResponse)?;
        if fields.len() != 2 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let releases = fields
            .get("sdk_libraries")
            .and_then(crate::client_submit::ResponseValue::as_array)
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let mut decoded = Vec::with_capacity(releases.len());
        let mut previous_release = 0;
        for release in releases {
            let mut fields = release
                .as_object()
                .cloned()
                .ok_or(ClientSubmitError::InvalidResponse)?;
            if fields.len() != 9 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let sdk_release = fields
                .get("release")
                .and_then(crate::client_submit::ResponseValue::as_number)
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value > previous_release)
                .ok_or(ClientSubmitError::InvalidResponse)?;
            previous_release = sdk_release;
            let native_abis = fields
                .get("native_abis")
                .and_then(crate::client_submit::ResponseValue::as_array)
                .ok_or(ClientSubmitError::InvalidResponse)?
                .iter()
                .map(|value| {
                    value
                        .as_number()
                        .and_then(|number| u8::try_from(number).ok())
                        .filter(|abi| *abi != 0)
                        .ok_or(ClientSubmitError::InvalidResponse)
                })
                .collect::<Result<Vec<_>, _>>()?;
            if native_abis.is_empty() || native_abis.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let target = response_string(&mut fields, "target")?;
            let cpu_baseline = response_string(&mut fields, "cpu_baseline")?;
            let runtime_profile = response_string(&mut fields, "runtime_profile")?;
            let soname = response_string(&mut fields, "soname")?;
            let sha256 = decode_lower_hex_32(&response_string(&mut fields, "sha256")?)?;
            let needed = fields
                .get("needed")
                .and_then(crate::client_submit::ResponseValue::as_array)
                .ok_or(ClientSubmitError::InvalidResponse)?
                .iter()
                .map(|value| {
                    value
                        .as_string()
                        .map(str::to_owned)
                        .ok_or(ClientSubmitError::InvalidResponse)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let library_bytes = fields
                .get("library_bytes")
                .and_then(crate::client_submit::ResponseValue::as_number)
                .filter(|value| *value > 0)
                .ok_or(ClientSubmitError::InvalidResponse)?;
            for key in ["release", "native_abis", "needed", "library_bytes"] {
                fields.remove(key);
            }
            if target.is_empty()
                || cpu_baseline.is_empty()
                || runtime_profile.is_empty()
                || soname != format!("libprdb_sql.so.{sdk_release}")
                || needed.iter().any(String::is_empty)
                || !fields.is_empty()
            {
                return Err(ClientSubmitError::InvalidResponse);
            }
            decoded.push(SdkLibraryInfo {
                sdk_release,
                native_abis,
                target,
                cpu_baseline,
                runtime_profile,
                soname,
                sha256,
                needed,
                library_bytes,
            });
        }
        let next_cursor = match fields.get("next_cursor") {
            Some(crate::client_submit::ResponseValue::Null) => None,
            Some(crate::client_submit::ResponseValue::String(cursor)) => {
                decode_metadata_cursor(cursor)?;
                Some(cursor.clone())
            }
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        Ok(MetadataPage {
            records: decoded,
            next_cursor,
        })
    }

    /// Stream a canonical repository metadata record and its SDK library to
    /// the database instance. The SDK bytes are sent in control-mode chunks,
    /// never as query-package or invocation data.
    pub fn install_sdk_library(
        &mut self,
        metadata: &[u8],
        library: &[u8],
    ) -> Result<SdkLibraryInfo, ClientSubmitError> {
        if metadata.is_empty()
            || metadata.len() > 64 * 1024
            || library.is_empty()
            || library.len() > 128 * 1024 * 1024
        {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let library_sha256: [u8; 32] = Sha256::digest(library).into();
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if operation_id == [0; 16] {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let prepare = format!(
            "{{\"phase\":\"prepare\",\"operation_version\":1,\"operation\":\"sdk_libraries.install\",\"arguments\":{{\"operation_id\":\"{}\",\"metadata_bytes\":\"{}\",\"library_bytes\":\"{}\",\"library_sha256\":\"{}\"}}}}",
            lower_hex(&operation_id),
            metadata.len(),
            library.len(),
            lower_hex(&library_sha256)
        );
        let prepared = match self.transport.exchange(10, prepare.into_bytes()) {
            Ok(response) => response,
            Err(ClientSubmitError::ServerRejected(message)) => {
                return Err(ClientSubmitError::ServerRejected(message));
            }
            Err(_) => {
                return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                    operation_id,
                ));
            }
        };
        let fields = parse_flat_response(&prepared.payload)?;
        if fields.len() != 2 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        if response_id(&fields, "operation_id")? != operation_id {
            return Err(ClientSubmitError::InvalidResponse);
        }

        let mut offset = response_decimal_u64(&fields, "next_offset")?;
        let starting_offset = offset;
        let mut stream_position = 0u64;
        for part in [metadata, library] {
            for data in part.chunks(CLIENT_MAX_CHUNK_BYTES) {
                let chunk_start = stream_position;
                stream_position = stream_position
                    .checked_add(data.len() as u64)
                    .ok_or(ClientSubmitError::InvalidResponse)?;
                if stream_position <= starting_offset {
                    continue;
                }
                let skip = usize::try_from(starting_offset.saturating_sub(chunk_start))
                    .map_err(|_| ClientSubmitError::InvalidResponse)?;
                if skip > data.len() {
                    return Err(ClientSubmitError::InvalidResponse);
                }
                let data = &data[skip..];
                let mut payload = Vec::with_capacity(24 + data.len());
                payload.extend_from_slice(&operation_id);
                payload.extend_from_slice(&offset.to_le_bytes());
                payload.extend_from_slice(data);
                let response = self
                    .transport
                    .exchange(
                        crate::client_protocol::CLIENT_SDK_LIBRARY_CHUNK_KIND,
                        payload,
                    )
                    .map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
                let fields = parse_flat_response(&response.payload)?;
                let next_offset = response_decimal_u64(&fields, "next_offset")?;
                offset = offset
                    .checked_add(data.len() as u64)
                    .ok_or(ClientSubmitError::InvalidResponse)?;
                if fields.len() != 1 || next_offset != offset {
                    return Err(ClientSubmitError::InvalidResponse);
                }
            }
        }

        let finish = format!(
            "{{\"phase\":\"write\",\"operation_version\":1,\"operation\":\"sdk_libraries.install.finish\",\"arguments\":{{\"operation_id\":\"{}\"}}}}",
            lower_hex(&operation_id)
        );
        let finished = match self.transport.exchange(10, finish.into_bytes()) {
            Ok(response) => response,
            Err(ClientSubmitError::ServerRejected(message)) => {
                return Err(ClientSubmitError::ServerRejected(message));
            }
            Err(_) => {
                return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                    operation_id,
                ));
            }
        };
        let fields = parse_flat_response(&finished.payload)
            .map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
        if fields.len() != 4 {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ));
        }
        let reported_operation = response_id(&fields, "operation_id")
            .map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
        let reported_bytes = fields
            .get("library_bytes")
            .and_then(ResponseValue::as_number)
            .ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?;
        if reported_operation != operation_id || reported_bytes != library.len() as u64 {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ));
        }
        let sdk_release = fields
            .get("release")
            .and_then(ResponseValue::as_number)
            .and_then(|value| u32::try_from(value).ok())
            .filter(|release| *release != 0)
            .ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?;
        let digest_text = fields
            .get("sha256")
            .and_then(ResponseValue::as_string)
            .ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))?;
        let digest = decode_lower_hex_32(digest_text)
            .map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
        if digest != library_sha256 {
            return Err(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ));
        }
        self.list_sdk_libraries()?
            .into_iter()
            .find(|release| release.sdk_release == sdk_release && release.sha256 == digest)
            .ok_or(ClientSubmitError::AdministrativeOutcomeUncertain(
                operation_id,
            ))
    }

    /// Remove one exact SDK release. The server checks current database-admin
    /// authority and refuses removal while any invocation holds a pin.
    pub fn remove_sdk_library(&mut self, sdk_release: u32) -> Result<u64, ClientSubmitError> {
        if sdk_release == 0 {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let mut operation_id = [0; 16];
        getrandom::fill(&mut operation_id)
            .map_err(|_| ClientSubmitError::InvalidAdministrativeRequest)?;
        if operation_id == [0; 16] {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let request = format!(
            "{{\"phase\":\"write\",\"operation_version\":1,\"operation\":\"sdk_libraries.remove\",\"arguments\":{{\"operation_id\":\"{}\",\"release\":\"{sdk_release}\"}}}}",
            lower_hex(&operation_id)
        );
        let response = self
            .transport
            .exchange(10, request.into_bytes())
            .map_err(|_| ClientSubmitError::AdministrativeOutcomeUncertain(operation_id))?;
        let fields = parse_flat_response(&response.payload)?;
        if fields.len() != 3
            || response_id(&fields, "operation_id")? != operation_id
            || fields.get("release").and_then(ResponseValue::as_number)
                != Some(u64::from(sdk_release))
        {
            return Err(ClientSubmitError::InvalidResponse);
        }
        fields
            .get("removed_bytes")
            .and_then(ResponseValue::as_number)
            .ok_or(ClientSubmitError::InvalidResponse)
    }

    pub fn sdk_library_operation_status(
        &mut self,
        operation_id: [u8; 16],
    ) -> Result<SdkLibraryOperationStatus, ClientSubmitError> {
        if operation_id == [0; 16] {
            return Err(ClientSubmitError::InvalidAdministrativeRequest);
        }
        let request = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"sdk_libraries.operation_status\",\"arguments\":{{\"operation_id\":\"{}\"}}}}",
            lower_hex(&operation_id)
        );
        let response = self.transport.exchange(10, request.into_bytes())?;
        let fields = parse_flat_response(&response.payload)?;
        if response_id(&fields, "operation_id")? != operation_id {
            return Err(ClientSubmitError::InvalidResponse);
        }
        match fields.get("state").and_then(ResponseValue::as_string) {
            Some("unknown") if fields.len() == 2 => Ok(SdkLibraryOperationStatus::Unknown),
            Some("succeeded") => {
                let release = fields
                    .get("release")
                    .and_then(ResponseValue::as_number)
                    .and_then(|value| u32::try_from(value).ok())
                    .filter(|value| *value > 0)
                    .ok_or(ClientSubmitError::InvalidResponse)?;
                match fields.get("operation").and_then(ResponseValue::as_string) {
                    Some("install") if fields.len() == 6 => {
                        Ok(SdkLibraryOperationStatus::Installed {
                            sdk_release: release,
                            sha256: decode_lower_hex_32(
                                fields
                                    .get("sha256")
                                    .and_then(ResponseValue::as_string)
                                    .ok_or(ClientSubmitError::InvalidResponse)?,
                            )?,
                            library_bytes: fields
                                .get("library_bytes")
                                .and_then(ResponseValue::as_number)
                                .filter(|value| *value > 0)
                                .ok_or(ClientSubmitError::InvalidResponse)?,
                        })
                    }
                    Some("remove") if fields.len() == 5 => Ok(SdkLibraryOperationStatus::Removed {
                        sdk_release: release,
                        removed_bytes: fields
                            .get("removed_bytes")
                            .and_then(ResponseValue::as_number)
                            .filter(|value| *value > 0)
                            .ok_or(ClientSubmitError::InvalidResponse)?,
                    }),
                    _ => Err(ClientSubmitError::InvalidResponse),
                }
            }
            _ => Err(ClientSubmitError::InvalidResponse),
        }
    }

    /// Return one best-effort page of this authenticated owner's live
    /// invocations. The server binds and revalidates every cursor to the owner.
    pub fn list_own_invocations_page(
        &mut self,
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<OwnInvocationPage, ClientSubmitError> {
        if !(1..=crate::storage_manager::MAX_OWN_INVOCATION_PAGE).contains(&page_limit)
            || cursor.is_some_and(|value| {
                crate::storage_manager::InvocationListCursor::from_wire(value).is_none()
            })
        {
            return Err(ClientSubmitError::InvalidMetadataPage);
        }
        let mut arguments = format!("\"limit\":\"{page_limit}\"");
        if let Some(cursor) = cursor {
            arguments.push_str(",\"cursor\":\"");
            arguments.push_str(cursor);
            arguments.push('"');
        }
        let payload = format!(
            "{{\"phase\":\"read\",\"operation_version\":2,\"operation\":\"runtime.invocations.own.list\",\"arguments\":{{{arguments}}}}}"
        );
        let response = self.transport.exchange(10, payload.into_bytes())?;
        let mut fields = parse_flat_response(&response.payload)?;
        if fields.len() != 4 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let observed_at_us = response_u64(&mut fields, "observed_at_us")?
            .filter(|value| *value > 0)
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let consistency = match fields.remove("consistency") {
            Some(ResponseValue::String(value)) if value == "best_effort" => {
                RuntimeConsistency::BestEffort
            }
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        let rows = match fields.remove("invocations") {
            Some(ResponseValue::Array(values)) => values,
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        if rows.len() > page_limit {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let mut records = Vec::with_capacity(rows.len());
        let mut previous_id = None;
        for value in rows {
            let ResponseValue::Object(mut row) = value else {
                return Err(ClientSubmitError::InvalidResponse);
            };
            if row.len() != 5 && row.len() != 7 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let invocation_id = response_id(&row, "invocation_id")?;
            row.remove("invocation_id");
            let state = match row.remove("state") {
                Some(ResponseValue::String(value)) if value == "active" => {
                    RuntimeInvocationState::Active
                }
                Some(ResponseValue::String(value)) if value == "committing" => {
                    RuntimeInvocationState::Committing
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let started_at_us = response_u64(&mut row, "started_at_us")?
                .ok_or(ClientSubmitError::InvalidResponse)?;
            let deadline_at_us = response_optional_u64(&mut row, "deadline_at_us")?;
            let cancellation_requested = response_boolean(&mut row, "cancellation_requested")?;
            let has_staged_rows = row.contains_key("staged_rows");
            let has_staged_value_bytes = row.contains_key("staged_value_bytes");
            if has_staged_rows != has_staged_value_bytes {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let staged_rows = response_u64(&mut row, "staged_rows")?.unwrap_or(0);
            let staged_value_bytes = response_u64(&mut row, "staged_value_bytes")?.unwrap_or(0);
            if !row.is_empty()
                || previous_id.is_some_and(|previous| previous >= invocation_id)
                || started_at_us == 0
                || deadline_at_us.is_some_and(|deadline| deadline == 0)
            {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_id = Some(invocation_id);
            records.push(OwnInvocation {
                invocation_id,
                state,
                started_at_us,
                deadline_at_us,
                cancellation_requested,
                staged_rows,
                staged_value_bytes,
            });
        }
        let next_cursor = match fields.remove("next_cursor") {
            None | Some(ResponseValue::Null) => None,
            Some(ResponseValue::String(value))
                if crate::storage_manager::InvocationListCursor::from_wire(&value).is_some() =>
            {
                Some(value)
            }
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        Ok(OwnInvocationPage {
            observed_at_us,
            consistency,
            records,
            next_cursor,
        })
    }

    /// Return one best-effort page of invocation registry state. The server checks
    /// current database-administrator authority on every page.
    pub fn list_runtime_invocations_page(
        &mut self,
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<RuntimeInvocationPage, ClientSubmitError> {
        if !(1..=crate::storage_manager::MAX_ADMIN_INVOCATION_PAGE).contains(&page_limit)
            || cursor.is_some_and(|value| decode_admin_runtime_cursor(value).is_none())
        {
            return Err(ClientSubmitError::InvalidMetadataPage);
        }
        let mut arguments = format!("\"limit\":\"{page_limit}\"");
        if let Some(cursor) = cursor {
            arguments.push_str(",\"cursor\":\"");
            arguments.push_str(cursor);
            arguments.push('"');
        }
        let payload = format!(
            "{{\"phase\":\"read\",\"operation_version\":8,\"operation\":\"runtime.invocations.list\",\"arguments\":{{{arguments}}}}}"
        );
        let response = self.transport.exchange(10, payload.into_bytes())?;
        let mut fields = parse_flat_response(&response.payload)?;
        if fields.len() != 22 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let observed_at_us = response_u64(&mut fields, "observed_at_us")?
            .filter(|value| *value > 0)
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let consistency = match fields.remove("consistency") {
            Some(ResponseValue::String(value)) if value == "best_effort" => {
                RuntimeConsistency::BestEffort
            }
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        let rows = match fields.remove("invocations") {
            Some(ResponseValue::Array(values)) => values,
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        if rows.len() > page_limit {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let mut records = Vec::with_capacity(rows.len());
        let mut previous_id = None;
        for value in rows {
            let ResponseValue::Object(mut row) = value else {
                return Err(ClientSubmitError::InvalidResponse);
            };
            if row.len() != 12 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let invocation_id = response_id(&row, "invocation_id")?;
            let transaction_id = response_id(&row, "transaction_id")?;
            let principal_id = response_id(&row, "principal_id")?;
            let state = match row.remove("state") {
                Some(ResponseValue::String(value)) if value == "active" => {
                    RuntimeInvocationState::Active
                }
                Some(ResponseValue::String(value)) if value == "committing" => {
                    RuntimeInvocationState::Committing
                }
                Some(ResponseValue::String(value)) if value == "committed" => {
                    RuntimeInvocationState::Committed
                }
                Some(ResponseValue::String(value)) if value == "aborted" => {
                    RuntimeInvocationState::Aborted
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let started_at_us = response_optional_u64(&mut row, "started_at_us")?;
            let age_us =
                response_u64(&mut row, "age_us")?.ok_or(ClientSubmitError::InvalidResponse)?;
            let deadline_at_us = response_optional_u64(&mut row, "deadline_at_us")?;
            let staged_rows =
                response_u64(&mut row, "staged_rows")?.ok_or(ClientSubmitError::InvalidResponse)?;
            let staged_value_bytes = response_u64(&mut row, "staged_value_bytes")?
                .ok_or(ClientSubmitError::InvalidResponse)?;
            let snapshot_pin_count = response_u64(&mut row, "snapshot_pin_count")?
                .ok_or(ClientSubmitError::InvalidResponse)?;
            let cancellation_requested = match row.remove("cancellation_requested") {
                Some(ResponseValue::Boolean(value)) => value,
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let cancellation_reason_code =
                response_optional_u16(&mut row, "cancellation_reason_code")?;
            let is_live = matches!(
                state,
                RuntimeInvocationState::Active | RuntimeInvocationState::Committing
            );
            if invocation_id == [0; 16]
                || transaction_id == [0; 16]
                || principal_id == [0; 16]
                || previous_id.is_some_and(|previous| previous >= invocation_id)
                || started_at_us.is_some_and(|value| value == 0)
                || deadline_at_us.is_some_and(|value| value == 0)
                || cancellation_reason_code.is_some_and(|value| value == 0)
                || is_live != started_at_us.is_some()
                || cancellation_requested != cancellation_reason_code.is_some()
                || (!is_live
                    && (deadline_at_us.is_some()
                        || cancellation_requested
                        || cancellation_reason_code.is_some()))
            {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_id = Some(invocation_id);
            records.push(RuntimeInvocation {
                invocation_id,
                transaction_id,
                principal_id,
                state,
                started_at_us,
                age_us,
                deadline_at_us,
                staged_rows,
                staged_value_bytes,
                snapshot_pin_count,
                cancellation_requested,
                cancellation_reason_code,
            });
        }
        let pinned_snapshot_count = response_u64(&mut fields, "pinned_snapshot_count")?
            .and_then(|value| usize::try_from(value).ok())
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let oldest_snapshot_pin_age_us = response_u64(&mut fields, "oldest_snapshot_pin_age_us")?
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let invocations_admitted_total = response_u64(&mut fields, "invocations_admitted_total")?
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let invocation_conflicts_total = response_u64(&mut fields, "invocation_conflicts_total")?
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let admin_cancellations_accepted_total =
            response_u64(&mut fields, "admin_cancellations_accepted_total")?
                .ok_or(ClientSubmitError::InvalidResponse)?;
        let admin_cancellations_denied_total =
            response_u64(&mut fields, "admin_cancellations_denied_total")?
                .ok_or(ClientSubmitError::InvalidResponse)?;
        let invocation_policy_denials_total =
            response_u64(&mut fields, "invocation_policy_denials_total")?
                .ok_or(ClientSubmitError::InvalidResponse)?;
        let invocation_staged_value_bytes_total =
            response_u64(&mut fields, "invocation_staged_value_bytes_total")?
                .ok_or(ClientSubmitError::InvalidResponse)?;
        let invocation_commit_latency_buckets =
            match fields.remove("invocation_commit_latency_buckets") {
                Some(ResponseValue::Array(values)) if values.len() == 6 => {
                    let mut buckets = [0; 6];
                    for (index, value) in values.into_iter().enumerate() {
                        let ResponseValue::Number(count) = value else {
                            return Err(ClientSubmitError::InvalidResponse);
                        };
                        buckets[index] = count;
                    }
                    buckets
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
        let invocation_redb_write_acquisition_buckets =
            match fields.remove("invocation_redb_write_acquisition_buckets") {
                Some(ResponseValue::Array(values)) if values.len() == 6 => {
                    let mut buckets = [0; 6];
                    for (index, value) in values.into_iter().enumerate() {
                        let ResponseValue::Number(count) = value else {
                            return Err(ClientSubmitError::InvalidResponse);
                        };
                        buckets[index] = count;
                    }
                    buckets
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
        let outcome_maintenance_passes_total =
            response_u64(&mut fields, "outcome_maintenance_passes_total")?
                .ok_or(ClientSubmitError::InvalidResponse)?;
        let outcome_maintenance_failures_total =
            response_u64(&mut fields, "outcome_maintenance_failures_total")?
                .ok_or(ClientSubmitError::InvalidResponse)?;
        let outcomes_pruned_total = response_u64(&mut fields, "outcomes_pruned_total")?
            .ok_or(ClientSubmitError::InvalidResponse)?;
        let gateway_result_bytes_relayed_total =
            response_u64(&mut fields, "gateway_result_bytes_relayed_total")?
                .ok_or(ClientSubmitError::InvalidResponse)?;
        let gateway_result_bytes_sent_to_clients_total =
            response_u64(&mut fields, "gateway_result_bytes_sent_to_clients_total")?
                .ok_or(ClientSubmitError::InvalidResponse)?;
        let mut supervisor_queue = match fields.remove("supervisor_queue") {
            Some(ResponseValue::Object(fields)) if fields.len() == 7 => fields,
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        let supervisor_queue = crate::supervisor::StagingRuntimeUsage {
            pending_submissions: response_u64(&mut supervisor_queue, "pending_submissions")?
                .ok_or(ClientSubmitError::InvalidResponse)?,
            starting_submissions: response_u64(&mut supervisor_queue, "starting_submissions")?
                .ok_or(ClientSubmitError::InvalidResponse)?,
            started_submissions: response_u64(&mut supervisor_queue, "started_submissions")?
                .ok_or(ClientSubmitError::InvalidResponse)?,
            queued_starts: response_u64(&mut supervisor_queue, "queued_starts")?
                .ok_or(ClientSubmitError::InvalidResponse)?,
            oldest_queued_start_wait_us: response_u64(
                &mut supervisor_queue,
                "oldest_queued_start_wait_us",
            )?
            .ok_or(ClientSubmitError::InvalidResponse)?,
            start_wait_buckets: response_u64_array(&mut supervisor_queue, "start_wait_buckets", 6)?,
            reserved_submission_bytes: response_u64(
                &mut supervisor_queue,
                "reserved_submission_bytes",
            )?
            .ok_or(ClientSubmitError::InvalidResponse)?,
        };
        if pinned_snapshot_count == 0 && oldest_snapshot_pin_age_us != 0 {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let fence_waits = match fields.remove("fence_waits") {
            Some(ResponseValue::Array(values))
                if values.len() <= crate::ddl_fence::MAX_DDL_FENCE_WAIT_PAGE =>
            {
                values
            }
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        let mut decoded_waits = Vec::with_capacity(fence_waits.len());
        let mut previous_wait_id = None;
        for value in fence_waits {
            let ResponseValue::Object(mut wait) = value else {
                return Err(ClientSubmitError::InvalidResponse);
            };
            if wait.len() != 7 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let wait_id = response_id(&wait, "wait_id")?;
            let category = match wait.remove("category") {
                Some(ResponseValue::String(value)) if value == "writer" => {
                    RuntimeFenceWaitCategory::Writer
                }
                Some(ResponseValue::String(value)) if value == "ddl" => {
                    RuntimeFenceWaitCategory::Ddl
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let waited_us =
                response_u64(&mut wait, "waited_us")?.ok_or(ClientSubmitError::InvalidResponse)?;
            let resource_ids = response_id_array(&mut wait, "resource_ids")?;
            let resources_truncated = response_boolean(&mut wait, "resources_truncated")?;
            let blocker_ids = response_id_array(&mut wait, "blocker_ids")?;
            let blockers_truncated = response_boolean(&mut wait, "blockers_truncated")?;
            if previous_wait_id.is_some_and(|previous| previous >= wait_id)
                || resource_ids.is_empty()
                || resource_ids.len() > crate::ddl_fence::MAX_DDL_FENCE_IDS_PER_WAIT
                || (resources_truncated
                    && resource_ids.len() != crate::ddl_fence::MAX_DDL_FENCE_IDS_PER_WAIT)
                || blocker_ids.is_empty()
                || blocker_ids.len() > crate::ddl_fence::MAX_DDL_FENCE_IDS_PER_WAIT
                || (blockers_truncated
                    && blocker_ids.len() != crate::ddl_fence::MAX_DDL_FENCE_IDS_PER_WAIT)
                || !strictly_ordered_ids(&resource_ids)
                || !strictly_ordered_ids(&blocker_ids)
            {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_wait_id = Some(wait_id);
            decoded_waits.push(RuntimeFenceWait {
                wait_id,
                category,
                waited_us,
                resource_ids,
                resources_truncated,
                blocker_ids,
                blockers_truncated,
            });
        }
        let fence_waits_truncated = response_boolean(&mut fields, "fence_waits_truncated")?;
        if fence_waits_truncated && decoded_waits.len() != crate::ddl_fence::MAX_DDL_FENCE_WAIT_PAGE
        {
            return Err(ClientSubmitError::InvalidResponse);
        }
        let next_cursor = match fields.remove("next_cursor") {
            None | Some(ResponseValue::Null) => None,
            Some(ResponseValue::String(value)) if decode_admin_runtime_cursor(&value).is_some() => {
                Some(value)
            }
            _ => return Err(ClientSubmitError::InvalidResponse),
        };
        Ok(RuntimeInvocationPage {
            observed_at_us,
            consistency,
            records,
            pinned_snapshot_count,
            oldest_snapshot_pin_age_us,
            invocations_admitted_total,
            invocation_conflicts_total,
            admin_cancellations_accepted_total,
            admin_cancellations_denied_total,
            invocation_policy_denials_total,
            invocation_staged_value_bytes_total,
            invocation_commit_latency_buckets,
            invocation_redb_write_acquisition_buckets,
            outcome_maintenance_passes_total,
            outcome_maintenance_failures_total,
            outcomes_pruned_total,
            fence_waits: decoded_waits,
            fence_waits_truncated,
            gateway_result_bytes_relayed_total,
            gateway_result_bytes_sent_to_clients_total,
            supervisor_queue,
            next_cursor,
        })
    }

    /// Return one ordered page of metadata visible to this authenticated
    /// control principal. Database administrators and schema owners receive
    /// their additional administrative visibility from the server.
    pub fn list_schemas_page(
        &mut self,
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::VisibleSchema>, ClientSubmitError> {
        validate_metadata_page_request(page_limit, cursor)?;
        let (records, next_cursor) = request_metadata_page(
            &mut self.transport,
            "metadata.schemas.list",
            metadata_page_arguments(page_limit, cursor),
        )?;
        let records = discovery_records(records)?;
        let mut schemas = Vec::with_capacity(records.len());
        let mut previous_id = None;
        for mut record in records {
            if record.len() != 2 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let schema_id = response_id(&record, "schema_id")?;
            let name = response_string(&mut record, "name")?;
            if name.is_empty() || previous_id.is_some_and(|previous| previous >= schema_id) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_id = Some(schema_id);
            schemas.push(crate::metadata_discovery::VisibleSchema { schema_id, name });
        }
        Ok(MetadataPage {
            records: schemas,
            next_cursor,
        })
    }

    /// Return one ordered relation page for a schema visible to this control
    /// principal. An empty page is also the unavailable-schema projection.
    pub fn list_relations_page(
        &mut self,
        schema_id: [u8; 16],
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::VisibleRelation>, ClientSubmitError> {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = format!(
            "{{\"schema_id\":\"{}\",{}}}",
            lower_hex(&schema_id),
            metadata_page_arguments_fields(page_limit, cursor)
        );
        let (records, next_cursor) =
            request_metadata_page(&mut self.transport, "metadata.relations.list", arguments)?;
        let records = discovery_records(records)?;
        let mut relations = Vec::with_capacity(records.len());
        let mut previous_id = None;
        for mut record in records {
            if record.len() != 2 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let relation_id = response_id(&record, "relation_id")?;
            let name = response_string(&mut record, "name")?;
            if name.is_empty() || previous_id.is_some_and(|previous| previous >= relation_id) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_id = Some(relation_id);
            relations.push(crate::metadata_discovery::VisibleRelation { relation_id, name });
        }
        Ok(MetadataPage {
            records: relations,
            next_cursor,
        })
    }

    /// Return one ordinal-ordered declared-column page for a visible relation.
    pub fn list_columns_page(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::VisibleColumn>, ClientSubmitError> {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = format!(
            "{{\"schema_id\":\"{}\",\"relation_id\":\"{}\",{}}}",
            lower_hex(&schema_id),
            lower_hex(&relation_id),
            metadata_page_arguments_fields(page_limit, cursor)
        );
        let (records, next_cursor) =
            request_metadata_page(&mut self.transport, "metadata.columns.list", arguments)?;
        let records = discovery_records(records)?;
        let mut columns = Vec::with_capacity(records.len());
        let mut previous_ordinal = None;
        for mut record in records {
            if record.len() != 5 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let column_id = response_id(&record, "column_id")?;
            let name = response_string(&mut record, "name")?;
            let ordinal = match record.remove("ordinal") {
                Some(ResponseValue::Number(value)) => {
                    u32::try_from(value).map_err(|_| ClientSubmitError::InvalidResponse)?
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let scalar_type_name = response_string(&mut record, "scalar_type")?;
            let scalar_type = crate::client_protocol::decode_table_scalar_type(&scalar_type_name)
                .map_err(|_| ClientSubmitError::InvalidResponse)?;
            let nullable = match record.remove("nullable") {
                Some(ResponseValue::Boolean(value)) => value,
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            if column_id == [0; 16]
                || name.is_empty()
                || previous_ordinal.is_some_and(|previous| previous >= ordinal)
            {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_ordinal = Some(ordinal);
            columns.push(crate::metadata_discovery::VisibleColumn {
                column_id,
                name,
                ordinal,
                scalar_type,
                nullable,
            });
        }
        Ok(MetadataPage {
            records: columns,
            next_cursor,
        })
    }

    /// Return one page of logical index definitions for an administratively
    /// visible table. Physical storage identifiers and memberships are not
    /// included in the response.
    pub fn list_indexes_page(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::AdministrativeIndex>, ClientSubmitError>
    {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = format!(
            "{{\"schema_id\":\"{}\",\"table_id\":\"{}\",{}}}",
            lower_hex(&schema_id),
            lower_hex(&table_id),
            metadata_page_arguments_fields(page_limit, cursor)
        );
        let (records, next_cursor) =
            request_metadata_page(&mut self.transport, "metadata.indexes.list", arguments)?;
        let records = discovery_records(records)?;
        let mut indexes = Vec::with_capacity(records.len());
        let mut previous_id = None;
        for mut record in records {
            if record.len() != 4 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let index_id = response_id(&record, "index_id")?;
            let name = response_string(&mut record, "name")?;
            let column_ids = match record.remove("column_ids") {
                Some(ResponseValue::Array(values)) if !values.is_empty() => values
                    .into_iter()
                    .map(|value| {
                        value
                            .as_string()
                            .and_then(decode_hex_array::<16>)
                            .filter(|id| *id != [0; 16])
                            .ok_or(ClientSubmitError::InvalidResponse)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let mut seen_columns = HashSet::new();
            if column_ids
                .iter()
                .any(|column_id| !seen_columns.insert(*column_id))
            {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let unique = match record.remove("unique") {
                Some(ResponseValue::Boolean(value)) => value,
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            if previous_id.is_some_and(|previous| previous >= index_id) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_id = Some(index_id);
            indexes.push(crate::metadata_discovery::AdministrativeIndex {
                index_id,
                name,
                column_ids,
                unique,
            });
        }
        Ok(MetadataPage {
            records: indexes,
            next_cursor,
        })
    }

    /// Return logical constraint definitions for an administratively visible
    /// table. CHECK expressions and hidden foreign-key targets are omitted.
    pub fn list_constraints_page(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::AdministrativeConstraint>, ClientSubmitError>
    {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = format!(
            "{{\"schema_id\":\"{}\",\"table_id\":\"{}\",{}}}",
            lower_hex(&schema_id),
            lower_hex(&table_id),
            metadata_page_arguments_fields(page_limit, cursor)
        );
        let (records, next_cursor) =
            request_metadata_page(&mut self.transport, "metadata.constraints.list", arguments)?;
        let records = discovery_records(records)?;
        let mut constraints = Vec::with_capacity(records.len());
        let mut previous_id = None;
        for mut record in records {
            if record.len() != 6 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let constraint_id = response_id(&record, "constraint_id")?;
            let name = take_string(&record, "name").ok_or(ClientSubmitError::InvalidResponse)?;
            validate_admin_name(&name)?;
            let kind = match record.remove("kind") {
                Some(ResponseValue::String(value)) => match value.as_str() {
                    "not_null" => crate::metadata_discovery::AdministrativeConstraintKind::NotNull,
                    "primary_key" => {
                        crate::metadata_discovery::AdministrativeConstraintKind::PrimaryKey
                    }
                    "unique" => crate::metadata_discovery::AdministrativeConstraintKind::Unique,
                    "check" => crate::metadata_discovery::AdministrativeConstraintKind::Check,
                    "foreign_key" => {
                        crate::metadata_discovery::AdministrativeConstraintKind::ForeignKey
                    }
                    _ => return Err(ClientSubmitError::InvalidResponse),
                },
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let decode_ids = |value: Option<ResponseValue>, allow_empty: bool| match value {
                Some(ResponseValue::Array(values)) if allow_empty || !values.is_empty() => values
                    .into_iter()
                    .map(|value| {
                        value
                            .as_string()
                            .and_then(decode_hex_array::<16>)
                            .filter(|id| *id != [0; 16])
                            .ok_or(ClientSubmitError::InvalidResponse)
                    })
                    .collect::<Result<Vec<_>, _>>(),
                _ => Err(ClientSubmitError::InvalidResponse),
            };
            let column_ids = decode_ids(record.remove("column_ids"), true)?;
            let referenced_table_id = match record.remove("referenced_table_id") {
                Some(ResponseValue::Null) => None,
                Some(ResponseValue::String(value)) => Some(
                    decode_hex_array::<16>(&value)
                        .filter(|id| *id != [0; 16])
                        .ok_or(ClientSubmitError::InvalidResponse)?,
                ),
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let referenced_column_ids = decode_ids(record.remove("referenced_column_ids"), true)?;
            let is_foreign_key =
                kind == crate::metadata_discovery::AdministrativeConstraintKind::ForeignKey;
            if (kind == crate::metadata_discovery::AdministrativeConstraintKind::Check
                && !column_ids.is_empty())
                || (kind != crate::metadata_discovery::AdministrativeConstraintKind::Check
                    && column_ids.is_empty())
                || (is_foreign_key
                    && referenced_table_id.is_some() == referenced_column_ids.is_empty())
                || (!is_foreign_key
                    && (referenced_table_id.is_some() || !referenced_column_ids.is_empty()))
                || previous_id.is_some_and(|previous| previous >= constraint_id)
            {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let mut seen_columns = HashSet::new();
            if column_ids.iter().any(|id| !seen_columns.insert(*id)) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let mut seen_referenced = HashSet::new();
            if referenced_column_ids
                .iter()
                .any(|id| !seen_referenced.insert(*id))
            {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_id = Some(constraint_id);
            constraints.push(crate::metadata_discovery::AdministrativeConstraint {
                constraint_id,
                name,
                kind,
                column_ids,
                referenced_table_id,
                referenced_column_ids,
            });
        }
        Ok(MetadataPage {
            records: constraints,
            next_cursor,
        })
    }

    /// Return one page of direct grants on an administratively visible table.
    /// The response contains IDs and grantor provenance, never principal names.
    pub fn list_grants_page(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::AdministrativeGrant>, ClientSubmitError>
    {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = format!(
            "{{\"schema_id\":\"{}\",\"table_id\":\"{}\",{}}}",
            lower_hex(&schema_id),
            lower_hex(&table_id),
            metadata_page_arguments_fields(page_limit, cursor)
        );
        let (records, next_cursor) =
            request_metadata_page(&mut self.transport, "metadata.grants.list", arguments)?;
        let records = discovery_records(records)?;
        let mut grants = Vec::with_capacity(records.len());
        let mut previous_key = None;
        for mut record in records {
            if record.len() != 5 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let grantee_id = response_id(&record, "grantee_id")?;
            let grantor_id = response_id(&record, "grantor_id")?;
            let privilege = match record.remove("privilege") {
                Some(ResponseValue::String(value)) => match value.as_str() {
                    "select" => crate::metadata_discovery::AdministrativeGrantPrivilege::Select,
                    "insert" => crate::metadata_discovery::AdministrativeGrantPrivilege::Insert,
                    "update" => crate::metadata_discovery::AdministrativeGrantPrivilege::Update,
                    "delete" => crate::metadata_discovery::AdministrativeGrantPrivilege::Delete,
                    "references" => {
                        crate::metadata_discovery::AdministrativeGrantPrivilege::References
                    }
                    "alter" => crate::metadata_discovery::AdministrativeGrantPrivilege::Alter,
                    "index" => crate::metadata_discovery::AdministrativeGrantPrivilege::Index,
                    "manage_policies" => {
                        crate::metadata_discovery::AdministrativeGrantPrivilege::ManagePolicies
                    }
                    "manage_grants" => {
                        crate::metadata_discovery::AdministrativeGrantPrivilege::ManageGrants
                    }
                    _ => return Err(ClientSubmitError::InvalidResponse),
                },
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let target_scope = match record.remove("target_scope") {
                Some(ResponseValue::String(value)) if value == "object" => {
                    crate::metadata_discovery::AdministrativeGrantTargetScope::Object
                }
                Some(ResponseValue::String(value)) if value == "column" => {
                    crate::metadata_discovery::AdministrativeGrantTargetScope::Column
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let target_id = response_id(&record, "target_id")?;
            let object_scope =
                target_scope == crate::metadata_discovery::AdministrativeGrantTargetScope::Object;
            let column_privilege = matches!(
                privilege,
                crate::metadata_discovery::AdministrativeGrantPrivilege::Select
                    | crate::metadata_discovery::AdministrativeGrantPrivilege::Insert
                    | crate::metadata_discovery::AdministrativeGrantPrivilege::Update
                    | crate::metadata_discovery::AdministrativeGrantPrivilege::References
            );
            if (object_scope && target_id != table_id) || (!object_scope && !column_privilege) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let privilege_order = match privilege {
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
            let scope_order = if object_scope { 1 } else { 2 };
            let key = (grantee_id, privilege_order, scope_order, target_id);
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_key = Some(key);
            grants.push(crate::metadata_discovery::AdministrativeGrant {
                grantee_id,
                grantor_id,
                privilege,
                target_scope,
                target_id,
            });
        }
        Ok(MetadataPage {
            records: grants,
            next_cursor,
        })
    }

    /// Return a secret-free administrative policy summary page. Expression
    /// IDs and bodies are never included in this control response.
    pub fn list_policies_page(
        &mut self,
        schema_id: [u8; 16],
        table_id: [u8; 16],
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::AdministrativePolicy>, ClientSubmitError>
    {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = format!(
            "{{\"schema_id\":\"{}\",\"table_id\":\"{}\",{}}}",
            lower_hex(&schema_id),
            lower_hex(&table_id),
            metadata_page_arguments_fields(page_limit, cursor)
        );
        let (records, next_cursor) =
            request_metadata_page(&mut self.transport, "metadata.policies.list", arguments)?;
        let records = discovery_records(records)?;
        let mut policies = Vec::with_capacity(records.len());
        let mut previous_id = None;
        for mut record in records {
            if record.len() != 6 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let policy_id = response_id(&record, "policy_id")?;
            let kind = match record.remove("kind") {
                Some(ResponseValue::String(value)) if value == "row" => {
                    crate::metadata_discovery::AdministrativePolicyKind::Row
                }
                Some(ResponseValue::String(value)) if value == "cell" => {
                    crate::metadata_discovery::AdministrativePolicyKind::Cell
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let operation = match record.remove("operation") {
                Some(ResponseValue::String(value)) if value == "select" => {
                    crate::metadata_discovery::AdministrativePolicyOperation::Select
                }
                Some(ResponseValue::String(value)) if value == "insert" => {
                    crate::metadata_discovery::AdministrativePolicyOperation::Insert
                }
                Some(ResponseValue::String(value)) if value == "update" => {
                    crate::metadata_discovery::AdministrativePolicyOperation::Update
                }
                Some(ResponseValue::String(value)) if value == "delete" => {
                    crate::metadata_discovery::AdministrativePolicyOperation::Delete
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let effect = match record.remove("effect") {
                Some(ResponseValue::String(value)) if value == "allow" => {
                    crate::metadata_discovery::AdministrativePolicyEffect::Allow
                }
                Some(ResponseValue::String(value)) if value == "restrict" => {
                    crate::metadata_discovery::AdministrativePolicyEffect::Restrict
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let column_ids = match record.remove("column_ids") {
                Some(ResponseValue::Array(values)) => values
                    .into_iter()
                    .map(|value| {
                        value
                            .as_string()
                            .and_then(decode_hex_array::<16>)
                            .filter(|id| *id != [0; 16])
                            .ok_or(ClientSubmitError::InvalidResponse)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let recipients = match record.remove("recipients") {
                Some(ResponseValue::Null) => None,
                Some(ResponseValue::Array(values)) if !values.is_empty() => {
                    let mut recipients = Vec::with_capacity(values.len());
                    let mut seen = HashSet::new();
                    for value in values {
                        let ResponseValue::Object(mut fields) = value else {
                            return Err(ClientSubmitError::InvalidResponse);
                        };
                        if fields.len() != 2 {
                            return Err(ClientSubmitError::InvalidResponse);
                        }
                        let kind = match fields.remove("kind") {
                            Some(ResponseValue::String(value)) if value == "user" => {
                                crate::metadata_discovery::AdministrativePolicyRecipientKind::User
                            }
                            Some(ResponseValue::String(value)) if value == "role" => {
                                crate::metadata_discovery::AdministrativePolicyRecipientKind::Role
                            }
                            _ => return Err(ClientSubmitError::InvalidResponse),
                        };
                        let principal_id = response_id(&fields, "principal_id")?;
                        if !seen.insert((kind as u8, principal_id)) {
                            return Err(ClientSubmitError::InvalidResponse);
                        }
                        recipients.push(crate::metadata_discovery::AdministrativePolicyRecipient {
                            kind,
                            principal_id,
                        });
                    }
                    if recipients.windows(2).any(|pair| {
                        (pair[0].kind as u8, pair[0].principal_id)
                            >= (pair[1].kind as u8, pair[1].principal_id)
                    }) {
                        return Err(ClientSubmitError::InvalidResponse);
                    }
                    Some(recipients)
                }
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            if (kind == crate::metadata_discovery::AdministrativePolicyKind::Row
                && !column_ids.is_empty())
                || (kind == crate::metadata_discovery::AdministrativePolicyKind::Cell
                    && (column_ids.is_empty()
                        || operation
                            == crate::metadata_discovery::AdministrativePolicyOperation::Delete))
                || column_ids.iter().copied().collect::<HashSet<_>>().len() != column_ids.len()
                || previous_id.is_some_and(|previous| previous >= policy_id)
            {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_id = Some(policy_id);
            policies.push(crate::metadata_discovery::AdministrativePolicy {
                policy_id,
                kind,
                operation,
                effect,
                column_ids,
                recipients,
            });
        }
        Ok(MetadataPage {
            records: policies,
            next_cursor,
        })
    }

    /// Return direct dependency edges whose two endpoints are visible to this
    /// administrative principal. Hidden endpoints are omitted server-side.
    pub fn list_dependencies_page(
        &mut self,
        object_id: [u8; 16],
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::AdministrativeDependency>, ClientSubmitError>
    {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = format!(
            "{{\"object_id\":\"{}\",{}}}",
            lower_hex(&object_id),
            metadata_page_arguments_fields(page_limit, cursor)
        );
        let (records, next_cursor) =
            request_metadata_page(&mut self.transport, "metadata.dependencies.list", arguments)?;
        let records = discovery_records(records)?;
        let mut dependencies = Vec::with_capacity(records.len());
        let mut previous_key = None;
        for mut record in records {
            if record.len() != 3 {
                return Err(ClientSubmitError::InvalidResponse);
            }
            let dependent_id = response_id(&record, "dependent_id")?;
            let referenced_id = response_id(&record, "referenced_id")?;
            let kind = match record.remove("kind") {
                Some(ResponseValue::String(value)) => match value.as_str() {
                    "owned_child" => {
                        crate::metadata_discovery::AdministrativeDependencyKind::OwnedChild
                    }
                    "expression_reference" => {
                        crate::metadata_discovery::AdministrativeDependencyKind::ExpressionReference
                    }
                    "foreign_key_target" => {
                        crate::metadata_discovery::AdministrativeDependencyKind::ForeignKeyTarget
                    }
                    "backing_index" => {
                        crate::metadata_discovery::AdministrativeDependencyKind::BackingIndex
                    }
                    "authority" => {
                        crate::metadata_discovery::AdministrativeDependencyKind::Authority
                    }
                    _ => return Err(ClientSubmitError::InvalidResponse),
                },
                _ => return Err(ClientSubmitError::InvalidResponse),
            };
            let key = (dependent_id, referenced_id, kind as u8);
            if previous_key.is_some_and(|previous| previous >= key) {
                return Err(ClientSubmitError::InvalidResponse);
            }
            previous_key = Some(key);
            dependencies.push(crate::metadata_discovery::AdministrativeDependency {
                dependent_id,
                referenced_id,
                kind,
            });
        }
        Ok(MetadataPage {
            records: dependencies,
            next_cursor,
        })
    }
}
