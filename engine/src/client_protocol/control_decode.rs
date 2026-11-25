use super::admin_decode::is_canonical_admin_principal_name;
use super::json::*;
use super::types::*;
use super::*;
use crate::credentials::TOKEN_SECRET_BYTES;
use zeroize::Zeroizing;

/// Decode the exact HELLO v1 JSON object. Keys and values are strictly typed
/// strings; duplicate and unknown keys are rejected.
pub fn decode_client_hello(payload: &[u8]) -> Result<ClientHello, ClientControlError> {
    let mut fields = parse_string_object(payload)?;
    let mode = match take_required(&mut fields, "mode")?.as_str() {
        "query" => ClientMode::Query,
        "control" => ClientMode::Control,
        _ => return Err(ClientControlError::InvalidMode),
    };
    let client_name = take_required(&mut fields, "client_name")?;
    if client_name.len() > 128 {
        return Err(ClientControlError::InvalidClientName);
    }
    let repository_sdk_library = match fields.remove("sdk_library_mode") {
        None => false,
        Some(mode) if mode == "repository" => true,
        Some(_) => return Err(ClientControlError::InvalidClientName),
    };
    if repository_sdk_library && mode != ClientMode::Query {
        return Err(ClientControlError::InvalidMode);
    }
    reject_unknown_fields(fields)?;
    Ok(ClientHello {
        mode,
        client_name,
        repository_sdk_library,
    })
}

/// Decode the exact AUTH v1 JSON object, requiring a lowercase opaque token
/// ID and canonical padded base64 for the fixed-size bearer secret.
pub fn decode_client_credentials(payload: &[u8]) -> Result<ClientCredentials, ClientControlError> {
    let mut fields = parse_string_object(payload)?;
    let token_id = decode_lower_hex_id(&take_required(&mut fields, "token_id")?)?;
    let secret_bytes = decode_canonical_base64(&take_required(&mut fields, "token_secret")?)?;
    let token_secret = secret_bytes
        .as_slice()
        .try_into()
        .map_err(|_| ClientControlError::InvalidTokenSecret)?;
    reject_unknown_fields(fields)?;
    Ok(ClientCredentials {
        token_id,
        token_secret,
    })
}

/// Decode ARTIFACT_CHUNK/PARAMETER_CHUNK as invocation ID, little-endian
/// contiguous offset, and 1–256 KiB of opaque data.
pub fn decode_client_chunk(payload: &[u8]) -> Result<ClientChunk, ClientControlError> {
    if !(25..=24 + CLIENT_MAX_CHUNK_BYTES).contains(&payload.len()) {
        return Err(ClientControlError::InvalidChunk);
    }
    let invocation_id: [u8; 16] = payload[..16]
        .try_into()
        .map_err(|_| ClientControlError::InvalidChunk)?;
    if invocation_id == [0; 16] {
        return Err(ClientControlError::InvalidChunk);
    }
    let offset = u64::from_le_bytes(
        payload[16..24]
            .try_into()
            .map_err(|_| ClientControlError::InvalidChunk)?,
    );
    let mut data = Vec::new();
    data.try_reserve_exact(payload.len() - 24)
        .map_err(|_| ClientControlError::InvalidChunk)?;
    data.extend_from_slice(&payload[24..]);
    Ok(ClientChunk {
        invocation_id,
        offset,
        data,
    })
}

/// Decode the exact PREPARE JSON schema, validating declared sizes and all
/// canonical representations before allocating decoded metadata buffers.
pub fn decode_client_prepare(payload: &[u8]) -> Result<ClientPrepare, ClientControlError> {
    decode_client_prepare_for_protocol(payload, 1)
}

/// Decode PREPARE using the negotiated protocol major. Protocol v2 permits a
/// package to bind its immutable staged bytes to a retained transaction; v1
/// rejects those fields and keeps its original one-shot semantics.
pub fn decode_client_prepare_for_protocol(
    payload: &[u8],
    protocol_major: u16,
) -> Result<ClientPrepare, ClientControlError> {
    if !matches!(protocol_major, 1 | 2) {
        return Err(ClientControlError::InvalidPrepare);
    }
    let mut fields = parse_scalar_object(payload)?;
    let manifest_version = take_number(&mut fields, "manifest_version")?
        .try_into()
        .map_err(|_| ClientControlError::InvalidPrepare)?;
    let manifest_text = take_string(&mut fields, "manifest")?;
    let artifact_bytes = take_decimal_string(&mut fields, "artifact_bytes")?;
    let artifact_sha256 = decode_lower_hex_hash(&take_string(&mut fields, "artifact_sha256")?)?;
    let repository_sdk_library = match fields.remove("sdk_library_mode") {
        None => false,
        Some(JsonScalar::String(value)) if value == "repository" => true,
        Some(_) => return Err(ClientControlError::InvalidPrepare),
    };
    let has_sql_library =
        fields.contains_key("sql_library_bytes") || fields.contains_key("sql_library_sha256");
    let parameter_bytes = take_decimal_string(&mut fields, "parameter_bytes")?;
    let parameter_sha256 = decode_lower_hex_hash(&take_string(&mut fields, "parameter_sha256")?)?;
    let sql_data_bytes = take_decimal_string(&mut fields, "sql_data_bytes")?;
    let sql_data_sha256 = decode_lower_hex_hash(&take_string(&mut fields, "sql_data_sha256")?)?;
    let scope_version = take_number(&mut fields, "scope_version")?
        .try_into()
        .map_err(|_| ClientControlError::InvalidPrepare)?;
    let scope_text = take_string(&mut fields, "requested_scope")?;
    let deadline_ms = take_decimal_string(&mut fields, "deadline_ms")?;
    let workload_class = match take_string(&mut fields, "workload_class")?.as_str() {
        "operational" => crate::supervisor::WorkloadClass::Operational,
        "analytical" => crate::supervisor::WorkloadClass::Analytical,
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    let transaction_id = fields
        .contains_key("transaction_id")
        .then(|| take_string(&mut fields, "transaction_id"))
        .transpose()?
        .map(|value| decode_lower_hex_id(&value).map_err(|_| ClientControlError::InvalidPrepare))
        .transpose()?;
    let package_id = fields
        .contains_key("package_id")
        .then(|| take_string(&mut fields, "package_id"))
        .transpose()?
        .map(|value| decode_lower_hex_id(&value).map_err(|_| ClientControlError::InvalidPrepare))
        .transpose()?;
    if transaction_id.is_some() != package_id.is_some()
        || protocol_major == 1 && transaction_id.is_some()
        || !repository_sdk_library
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    reject_unknown_scalars(fields)?;
    if artifact_bytes == 0
        || artifact_bytes > crate::supervisor::MAX_ARTIFACT_BYTES
        || has_sql_library
        || parameter_bytes > crate::supervisor::MAX_PARAMETER_BYTES
        || sql_data_bytes > crate::supervisor::MAX_SQL_DATA_BYTES
        || deadline_ms == 0
        || manifest_text.len() > crate::supervisor::MAX_MANIFEST_BYTES.div_ceil(3) * 4
        || scope_text.len() > crate::supervisor::MAX_SCOPE_BYTES.div_ceil(3) * 4
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    let manifest = decode_base64_bytes(&manifest_text)
        .filter(|bytes| !bytes.is_empty() && bytes.len() <= crate::supervisor::MAX_MANIFEST_BYTES)
        .ok_or(ClientControlError::InvalidPrepare)?;
    let requested_scope = decode_base64_bytes(&scope_text)
        .filter(|bytes| !bytes.is_empty() && bytes.len() <= crate::supervisor::MAX_SCOPE_BYTES)
        .ok_or(ClientControlError::InvalidPrepare)?;
    Ok(ClientPrepare {
        manifest_version,
        manifest,
        artifact_bytes,
        artifact_sha256,
        repository_sdk_library,
        parameter_bytes,
        parameter_sha256,
        sql_data_bytes,
        sql_data_sha256,
        scope_version,
        requested_scope,
        deadline_ms,
        workload_class,
        transaction_id,
        package_id,
    })
}

pub fn decode_client_seal(payload: &[u8]) -> Result<[u8; 16], ClientControlError> {
    let mut fields = parse_string_object(payload)?;
    let invocation_id = decode_lower_hex_id(&take_required(&mut fields, "invocation_id")?)
        .map_err(|_| ClientControlError::InvalidInvocationId)?;
    reject_unknown_fields(fields)?;
    Ok(invocation_id)
}

pub struct ClientStartPayload {
    pub invocation_id: [u8; 16],
    pub transaction_id: Option<[u8; 16]>,
    pub package_id: Option<[u8; 16]>,
    pub actor: Option<ClientStartActorCredentials>,
}

pub struct ClientStartActorCredentials {
    pub expected_name: String,
    pub token_id: [u8; 16],
    pub token_secret: Zeroizing<[u8; TOKEN_SECRET_BYTES]>,
}

pub(crate) struct ValidatedInteractiveStartCredentials {
    pub controller_token_id: [u8; 16],
    pub controller_token_secret: Zeroizing<[u8; TOKEN_SECRET_BYTES]>,
    pub actor: Option<ClientStartActorCredentials>,
}

/// Decode START identity fields under the negotiated protocol. Explicit v2
/// packages must echo the PREPARE transaction/package binding; v1 cannot
/// carry these fields.
pub fn decode_client_start_for_protocol(
    payload: &[u8],
    protocol_major: u16,
) -> Result<ClientStartPayload, ClientControlError> {
    if !matches!(protocol_major, 1 | 2) || payload.len() > CLIENT_MAX_CONTROL_BYTES {
        return Err(ClientControlError::InvalidInvocationId);
    }
    let mut fields = parse_string_object(payload)?;
    let invocation_id = decode_lower_hex_id(&take_required(&mut fields, "invocation_id")?)
        .map_err(|_| ClientControlError::InvalidInvocationId)?;
    let transaction_id = fields
        .remove("transaction_id")
        .map(|value| decode_lower_hex_id(&value).map_err(|_| ClientControlError::InvalidPrepare))
        .transpose()?;
    let package_id = fields
        .remove("package_id")
        .map(|value| decode_lower_hex_id(&value).map_err(|_| ClientControlError::InvalidPrepare))
        .transpose()?;
    let has_actor_name = fields.contains_key("expected_actor_name");
    let has_actor_token_id = fields.contains_key("actor_token_id");
    let has_actor_token_secret = fields.contains_key("actor_token_secret");
    let actor = match (has_actor_name, has_actor_token_id, has_actor_token_secret) {
        (false, false, false) => None,
        (true, true, true) if protocol_major == 2 => {
            let expected_name = take_required(&mut fields, "expected_actor_name")?;
            if !is_canonical_admin_principal_name(&expected_name) {
                return Err(ClientControlError::InvalidPrepare);
            }
            let token_id = decode_lower_hex_id(&take_required(&mut fields, "actor_token_id")?)
                .map_err(|_| ClientControlError::InvalidPrepare)?;
            let secret_text = Zeroizing::new(take_required(&mut fields, "actor_token_secret")?);
            let secret_bytes = decode_canonical_base64(&secret_text)?;
            let token_secret = <[u8; TOKEN_SECRET_BYTES]>::try_from(secret_bytes.as_slice())
                .map(Zeroizing::new)
                .map_err(|_| ClientControlError::InvalidTokenSecret)?;
            Some(ClientStartActorCredentials {
                expected_name,
                token_id,
                token_secret,
            })
        }
        _ => return Err(ClientControlError::InvalidPrepare),
    };
    if transaction_id.is_some() != package_id.is_some()
        || protocol_major == 1 && transaction_id.is_some()
    {
        return Err(ClientControlError::InvalidPrepare);
    }
    reject_unknown_fields(fields)?;
    Ok(ClientStartPayload {
        invocation_id,
        transaction_id,
        package_id,
        actor,
    })
}
