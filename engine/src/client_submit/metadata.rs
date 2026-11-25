use super::codec::*;
use super::response::*;
use super::*;
use std::net::SocketAddr;
use std::time::Duration;

impl MetadataClient {
    /// Connect over TCP, negotiate control mode, and authenticate. The caller's
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

    /// Return schema records from the authenticated principal's visible set.
    pub fn list_schemas(
        &mut self,
    ) -> Result<Vec<crate::metadata_discovery::VisibleSchema>, ClientSubmitError> {
        let mut schemas = Vec::new();
        let mut cursor = None;
        let mut previous_id = None;
        loop {
            let page = self.list_schemas_page(DEFAULT_METADATA_PAGE_SIZE, cursor.as_deref())?;
            for schema in &page.records {
                if previous_id.is_some_and(|previous| previous >= schema.schema_id) {
                    return Err(ClientSubmitError::InvalidResponse);
                }
                previous_id = Some(schema.schema_id);
            }
            schemas.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                return Ok(schemas);
            }
        }
    }

    /// Return one ordered page of visible schemas. Continuations are opaque,
    /// session-bound cursors issued by the authenticated control connection.
    pub fn list_schemas_page(
        &mut self,
        page_limit: usize,
        cursor: Option<&str>,
    ) -> Result<MetadataPage<crate::metadata_discovery::VisibleSchema>, ClientSubmitError> {
        validate_metadata_page_request(page_limit, cursor)?;
        let arguments = metadata_page_arguments(page_limit, cursor);
        let (records, next_cursor) =
            self.request_metadata_page("metadata.schemas.list", arguments)?;
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

    /// Return grant-visible relations for one schema. An empty vector means
    /// either an authorized empty schema or an unavailable schema.
    pub fn list_relations(
        &mut self,
        schema_id: [u8; 16],
    ) -> Result<Vec<crate::metadata_discovery::VisibleRelation>, ClientSubmitError> {
        let mut relations = Vec::new();
        let mut cursor = None;
        let mut previous_id = None;
        loop {
            let page =
                self.list_relations_page(schema_id, DEFAULT_METADATA_PAGE_SIZE, cursor.as_deref())?;
            for relation in &page.records {
                if previous_id.is_some_and(|previous| previous >= relation.relation_id) {
                    return Err(ClientSubmitError::InvalidResponse);
                }
                previous_id = Some(relation.relation_id);
            }
            relations.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                return Ok(relations);
            }
        }
    }

    /// Return one grant-filtered relation page for a schema.
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
            self.request_metadata_page("metadata.relations.list", arguments)?;
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

    /// Return the declared, grant-visible column shape for one relation.
    pub fn list_columns(
        &mut self,
        schema_id: [u8; 16],
        relation_id: [u8; 16],
    ) -> Result<Vec<crate::metadata_discovery::VisibleColumn>, ClientSubmitError> {
        let mut columns = Vec::new();
        let mut cursor = None;
        let mut previous_ordinal = None;
        loop {
            let page = self.list_columns_page(
                schema_id,
                relation_id,
                DEFAULT_METADATA_PAGE_SIZE,
                cursor.as_deref(),
            )?;
            for column in &page.records {
                if previous_ordinal.is_some_and(|previous| previous >= column.ordinal) {
                    return Err(ClientSubmitError::InvalidResponse);
                }
                previous_ordinal = Some(column.ordinal);
            }
            columns.extend(page.records);
            cursor = page.next_cursor;
            if cursor.is_none() {
                return Ok(columns);
            }
        }
    }

    /// Return one ordinal-ordered declared-column page for a relation.
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
            self.request_metadata_page("metadata.columns.list", arguments)?;
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

    pub(super) fn request_metadata_page(
        &mut self,
        operation: &str,
        arguments: String,
    ) -> Result<(Vec<ResponseValue>, Option<String>), ClientSubmitError> {
        let payload = format!(
            "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"{operation}\",\"arguments\":{arguments}}}"
        );
        let response = self.transport.exchange(10, payload.into_bytes())?;
        discovery_page_value(&response.payload, operation)
    }
}

pub(super) fn request_metadata_page(
    transport: &mut SubmissionClient,
    operation: &str,
    arguments: String,
) -> Result<(Vec<ResponseValue>, Option<String>), ClientSubmitError> {
    let payload = format!(
        "{{\"phase\":\"read\",\"operation_version\":1,\"operation\":\"{operation}\",\"arguments\":{arguments}}}"
    );
    let response = transport.exchange(10, payload.into_bytes())?;
    discovery_page_value(&response.payload, operation)
}
