//! Canonical catalog payloads.

const CATALOG_HEADER_LEN: usize = 32;
pub const MAX_CATALOG_ENVELOPE_BYTES: usize = 1_048_576;
const MAX_CATALOG_FIELDS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum WireType {
    U16 = 1,
    U32 = 2,
    U64 = 3,
    I64 = 4,
    Bool = 5,
    Id = 6,
    Text = 7,
    Bytes = 8,
    Ids = 9,
}

impl TryFrom<u8> for WireType {
    type Error = InvalidCatalogRecord;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::U16),
            2 => Ok(Self::U32),
            3 => Ok(Self::U64),
            4 => Ok(Self::I64),
            5 => Ok(Self::Bool),
            6 => Ok(Self::Id),
            7 => Ok(Self::Text),
            8 => Ok(Self::Bytes),
            9 => Ok(Self::Ids),
            _ => Err(InvalidCatalogRecord),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogField {
    pub tag: u16,
    pub wire_type: WireType,
    pub payload: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FieldDefinition {
    pub tag: u16,
    pub wire_type: WireType,
    pub required: bool,
    /// For IDS fields, require sorted unique IDs when the catalog field is a set.
    pub ids_are_set: bool,
}

pub struct FamilySchema<'a> {
    pub family_code: u16,
    pub fields: &'a [FieldDefinition],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogRecord {
    pub family_code: u16,
    pub logical_key: Vec<u8>,
    pub fields: Vec<CatalogField>,
}

const fn field(tag: u16, wire_type: WireType, required: bool) -> FieldDefinition {
    FieldDefinition {
        tag,
        wire_type,
        required,
        ids_are_set: false,
    }
}

const fn id_set(tag: u16) -> FieldDefinition {
    FieldDefinition {
        tag,
        wire_type: WireType::Ids,
        required: true,
        ids_are_set: true,
    }
}

static DATABASE_FIELDS: [FieldDefinition; 6] = [
    field(1, WireType::Text, true),
    field(2, WireType::U32, true),
    field(3, WireType::Bytes, true),
    field(4, WireType::Id, false),
    field(5, WireType::U32, true),
    field(6, WireType::U32, true),
];
static OBJECT_FIELDS: [FieldDefinition; 6] = [
    field(1, WireType::U16, true),
    field(2, WireType::Id, false),
    field(3, WireType::Text, true),
    field(4, WireType::Id, true),
    field(5, WireType::U64, true),
    field(6, WireType::U16, true),
];
static SCHEMA_FIELDS: [FieldDefinition; 1] = [field(1, WireType::Ids, true)];
static TYPE_FIELDS: [FieldDefinition; 6] = [
    field(1, WireType::U16, true),
    field(2, WireType::U16, true),
    field(3, WireType::U32, true),
    field(4, WireType::U32, true),
    field(5, WireType::U32, true),
    field(6, WireType::U32, true),
];
static TABLE_FIELDS: [FieldDefinition; 5] = [
    field(1, WireType::Id, true),
    field(2, WireType::U64, true),
    field(3, WireType::U64, true),
    field(4, WireType::Id, true),
    field(5, WireType::U16, true),
];
static COLUMN_FIELDS: [FieldDefinition; 10] = [
    field(1, WireType::Id, true),
    field(2, WireType::U32, true),
    field(3, WireType::Text, true),
    field(4, WireType::Id, true),
    field(5, WireType::Bytes, true),
    field(6, WireType::Bool, true),
    field(7, WireType::Id, false),
    field(8, WireType::U16, true),
    field(9, WireType::U64, true),
    field(10, WireType::Id, false),
];
static EXPRESSION_FIELDS: [FieldDefinition; 6] = [
    field(1, WireType::U16, true),
    field(2, WireType::U32, true),
    field(3, WireType::Id, true),
    field(4, WireType::Bytes, true),
    field(5, WireType::Bytes, true),
    id_set(6),
];
static CONSTRAINT_FIELDS: [FieldDefinition; 11] = [
    field(1, WireType::Id, true),
    field(2, WireType::U16, true),
    field(3, WireType::Ids, true),
    field(4, WireType::Id, false),
    field(5, WireType::Id, false),
    field(6, WireType::Ids, true),
    field(7, WireType::U16, true),
    field(8, WireType::U16, true),
    field(9, WireType::U16, true),
    field(10, WireType::U16, true),
    field(11, WireType::Id, false),
];
static INDEX_FIELDS: [FieldDefinition; 10] = [
    field(1, WireType::Id, true),
    field(2, WireType::Ids, true),
    field(3, WireType::U16, true),
    field(4, WireType::Bool, true),
    field(5, WireType::U16, true),
    field(6, WireType::Id, false),
    field(7, WireType::Id, true),
    field(8, WireType::U16, true),
    field(9, WireType::U32, true),
    field(10, WireType::U64, true),
];
static PRINCIPAL_FIELDS: [FieldDefinition; 4] = [
    field(1, WireType::U16, true),
    field(2, WireType::Text, true),
    field(3, WireType::Bool, true),
    field(4, WireType::U64, true),
];
static MEMBERSHIP_FIELDS: [FieldDefinition; 2] =
    [field(1, WireType::Id, true), field(2, WireType::U64, true)];
static GRANT_FIELDS: [FieldDefinition; 2] =
    [field(1, WireType::Id, true), field(2, WireType::U64, true)];
static POLICY_FIELDS: [FieldDefinition; 7] = [
    field(1, WireType::Id, true),
    field(2, WireType::U16, true),
    field(3, WireType::U16, true),
    field(4, WireType::U16, true),
    field(5, WireType::Id, false),
    field(6, WireType::Id, false),
    field(7, WireType::U64, true),
];
static NO_FIELDS: [FieldDefinition; 0] = [];
static VERIFIER_FIELDS: [FieldDefinition; 10] = [
    field(1, WireType::Id, true),
    field(2, WireType::U16, true),
    field(3, WireType::U16, true),
    field(4, WireType::Bytes, true),
    field(5, WireType::I64, true),
    field(6, WireType::I64, true),
    field(7, WireType::Bool, true),
    field(8, WireType::I64, false),
    field(9, WireType::Text, true),
    field(10, WireType::U64, true),
];

/// Return the v1 field registry for one catalog family.
pub fn family_schema(family_code: u16) -> Option<FamilySchema<'static>> {
    let fields: &'static [FieldDefinition] = match family_code {
        1 => &DATABASE_FIELDS,
        2 => &OBJECT_FIELDS,
        3 => &SCHEMA_FIELDS,
        4 => &TYPE_FIELDS,
        5 => &TABLE_FIELDS,
        6 => &COLUMN_FIELDS,
        7 => &EXPRESSION_FIELDS,
        8 => &CONSTRAINT_FIELDS,
        9 => &INDEX_FIELDS,
        10 => &PRINCIPAL_FIELDS,
        11 => &MEMBERSHIP_FIELDS,
        12 => &GRANT_FIELDS,
        13 => &POLICY_FIELDS,
        14 | 15 => &NO_FIELDS,
        16 => &VERIFIER_FIELDS,
        _ => return None,
    };
    Some(FamilySchema {
        family_code,
        fields,
    })
}

#[derive(Debug, PartialEq, Eq)]
pub struct PolicyTarget {
    pub policy_id: [u8; 16],
    pub kind: u16,
    pub target_id: [u8; 16],
}

#[derive(Debug, PartialEq, Eq)]
pub struct InvalidCatalogRecord;

impl std::fmt::Display for InvalidCatalogRecord {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("invalid catalog record")
    }
}

impl std::error::Error for InvalidCatalogRecord {}

fn validate_schema(schema: &FamilySchema<'_>) -> Result<(), InvalidCatalogRecord> {
    if !(1..=16).contains(&schema.family_code) || schema.fields.len() > MAX_CATALOG_FIELDS {
        return Err(InvalidCatalogRecord);
    }
    let mut previous = 0;
    for field in schema.fields {
        if field.tag == 0 || field.tag <= previous {
            return Err(InvalidCatalogRecord);
        }
        previous = field.tag;
    }
    Ok(())
}

fn validate_key(family_code: u16, logical_key: &[u8]) -> Result<(), InvalidCatalogRecord> {
    if logical_key.len() < 2 || logical_key[..2] != family_code.to_be_bytes() {
        return Err(InvalidCatalogRecord);
    }
    let components = &logical_key[2..];
    let valid_id = |bytes: &[u8]| bytes.len() == 17 && bytes[0] == 1 && bytes[1..] != [0; 16];
    let valid_u16 = |bytes: &[u8]| bytes.len() == 3 && bytes[0] == 1;
    let valid = match family_code {
        1..=10 | 13 | 16 => valid_id(components),
        11 => components.len() == 34 && valid_id(&components[..17]) && valid_id(&components[17..]),
        12 => {
            components.len() == 57
                && valid_id(&components[..17])
                && valid_id(&components[17..34])
                && valid_u16(&components[34..37])
                && (1..=15).contains(&u16::from_be_bytes([components[35], components[36]]))
                && valid_u16(&components[37..40])
                && matches!(u16::from_be_bytes([components[38], components[39]]), 1 | 2)
                && valid_id(&components[40..57])
                && (u16::from_be_bytes([components[38], components[39]]) != 1
                    || components[17..34] == components[40..57])
        }
        14 => {
            components.len() == 37
                && valid_id(&components[..17])
                && valid_u16(&components[17..20])
                && (1..=3).contains(&u16::from_be_bytes([components[18], components[19]]))
                && valid_id(&components[20..])
        }
        15 => {
            components.len() == 37
                && valid_id(&components[..17])
                && valid_id(&components[17..34])
                && valid_u16(&components[34..])
                && (1..=5).contains(&u16::from_be_bytes([components[35], components[36]]))
        }
        _ => false,
    };
    if !valid || logical_key.len() > MAX_CATALOG_ENVELOPE_BYTES - CATALOG_HEADER_LEN {
        return Err(InvalidCatalogRecord);
    }
    Ok(())
}

fn validate_field_payload(
    field: &CatalogField,
    ids_are_set: bool,
) -> Result<(), InvalidCatalogRecord> {
    let payload = field.payload.as_slice();
    let valid = match field.wire_type {
        WireType::U16 => payload.len() == 2,
        WireType::U32 => payload.len() == 4,
        WireType::U64 | WireType::I64 => payload.len() == 8,
        WireType::Bool => matches!(payload, [0] | [1]),
        WireType::Id => payload.len() == 16 && payload != [0; 16],
        WireType::Text => payload.len() <= 4096 && std::str::from_utf8(payload).is_ok(),
        WireType::Bytes => payload.len() <= 256 * 1024,
        WireType::Ids => {
            if payload.len() < 4 {
                false
            } else {
                let count = u32::from_le_bytes(payload[..4].try_into().unwrap()) as usize;
                let ids = payload[4..].chunks_exact(16).collect::<Vec<_>>();
                count <= 256
                    && payload.len() == 4 + count * 16
                    && ids.iter().all(|id| *id != [0; 16])
                    && (!ids_are_set || ids.windows(2).all(|pair| pair[0] < pair[1]))
            }
        }
    };
    if valid {
        Ok(())
    } else {
        Err(InvalidCatalogRecord)
    }
}

fn check_fields(
    schema: &FamilySchema<'_>,
    fields: &[CatalogField],
) -> Result<(), InvalidCatalogRecord> {
    if fields.len() > MAX_CATALOG_FIELDS {
        return Err(InvalidCatalogRecord);
    }
    let mut previous = 0;
    for field in fields {
        if field.tag == 0 || field.tag <= previous {
            return Err(InvalidCatalogRecord);
        }
        previous = field.tag;
        let definition = schema
            .fields
            .iter()
            .find(|definition| definition.tag == field.tag)
            .ok_or(InvalidCatalogRecord)?;
        if definition.wire_type != field.wire_type {
            return Err(InvalidCatalogRecord);
        }
        validate_field_payload(field, definition.ids_are_set)?;
    }
    if schema.fields.iter().any(|definition| {
        definition.required && !fields.iter().any(|field| field.tag == definition.tag)
    }) {
        return Err(InvalidCatalogRecord);
    }
    Ok(())
}

/// Encode one CAT1 record with a family-specific registry for known and required fields.
pub fn encode_envelope(
    schema: &FamilySchema<'_>,
    logical_key: &[u8],
    fields: &[CatalogField],
) -> Result<Vec<u8>, InvalidCatalogRecord> {
    validate_schema(schema)?;
    validate_key(schema.family_code, logical_key)?;
    check_fields(schema, fields)?;
    let total = fields
        .iter()
        .try_fold(CATALOG_HEADER_LEN + logical_key.len(), |length, field| {
            length.checked_add(8)?.checked_add(field.payload.len())
        })
        .ok_or(InvalidCatalogRecord)?;
    if total > MAX_CATALOG_ENVELOPE_BYTES {
        return Err(InvalidCatalogRecord);
    }
    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(b"CAT1");
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&schema.family_code.to_le_bytes());
    bytes.extend_from_slice(&(total as u32).to_le_bytes());
    bytes.extend_from_slice(&(logical_key.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&(fields.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&[0; 8]);
    bytes.extend_from_slice(logical_key);
    for field in fields {
        bytes.extend_from_slice(&field.tag.to_le_bytes());
        bytes.push(field.wire_type as u8);
        bytes.push(0);
        bytes.extend_from_slice(&(field.payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&field.payload);
    }
    Ok(bytes)
}

/// Decode one complete CAT1 record, rejecting unknown fields and noncanonical framing.
pub fn decode_envelope(
    schema: &FamilySchema<'_>,
    bytes: &[u8],
) -> Result<CatalogRecord, InvalidCatalogRecord> {
    validate_schema(schema)?;
    if bytes.len() < CATALOG_HEADER_LEN || bytes.len() > MAX_CATALOG_ENVELOPE_BYTES {
        return Err(InvalidCatalogRecord);
    }
    if &bytes[..4] != b"CAT1"
        || u16::from_le_bytes(bytes[4..6].try_into().unwrap()) != 1
        || u16::from_le_bytes(bytes[6..8].try_into().unwrap()) != schema.family_code
        || u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize != bytes.len()
        || u32::from_le_bytes(bytes[20..24].try_into().unwrap()) != 0
        || bytes[24..32] != [0; 8]
    {
        return Err(InvalidCatalogRecord);
    }
    let key_len = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let field_count = u32::from_le_bytes(bytes[16..20].try_into().unwrap()) as usize;
    if field_count > MAX_CATALOG_FIELDS {
        return Err(InvalidCatalogRecord);
    }
    let key_end = CATALOG_HEADER_LEN
        .checked_add(key_len)
        .filter(|&end| end <= bytes.len())
        .ok_or(InvalidCatalogRecord)?;
    let logical_key = bytes[CATALOG_HEADER_LEN..key_end].to_vec();
    validate_key(schema.family_code, &logical_key)?;

    let mut fields = Vec::with_capacity(field_count);
    let mut offset = key_end;
    for _ in 0..field_count {
        let header_end = offset
            .checked_add(8)
            .filter(|&end| end <= bytes.len())
            .ok_or(InvalidCatalogRecord)?;
        let tag = u16::from_le_bytes(bytes[offset..offset + 2].try_into().unwrap());
        let wire_type = WireType::try_from(bytes[offset + 2])?;
        if bytes[offset + 3] != 0 {
            return Err(InvalidCatalogRecord);
        }
        let payload_len =
            u32::from_le_bytes(bytes[offset + 4..header_end].try_into().unwrap()) as usize;
        offset = header_end;
        let payload_end = offset
            .checked_add(payload_len)
            .filter(|&end| end <= bytes.len())
            .ok_or(InvalidCatalogRecord)?;
        fields.push(CatalogField {
            tag,
            wire_type,
            payload: bytes[offset..payload_end].to_vec(),
        });
        offset = payload_end;
    }
    if offset != bytes.len() {
        return Err(InvalidCatalogRecord);
    }
    check_fields(schema, &fields)?;
    Ok(CatalogRecord {
        family_code: schema.family_code,
        logical_key,
        fields,
    })
}

/// Decode a CAT1 record using the registered v1 schema selected by its header.
pub fn decode_registered_envelope(bytes: &[u8]) -> Result<CatalogRecord, InvalidCatalogRecord> {
    let family_bytes = bytes.get(6..8).ok_or(InvalidCatalogRecord)?;
    let family_code = u16::from_le_bytes(family_bytes.try_into().unwrap());
    let schema = family_schema(family_code).ok_or(InvalidCatalogRecord)?;
    decode_envelope(&schema, bytes)
}

/// Encode one v1 policy-target record, including its canonical logical key.
pub fn encode_policy_target(record: &PolicyTarget) -> Result<Vec<u8>, InvalidCatalogRecord> {
    if record.policy_id == [0; 16] || record.target_id == [0; 16] || !(1..=3).contains(&record.kind)
    {
        return Err(InvalidCatalogRecord);
    }
    let mut bytes = Vec::with_capacity(71);
    bytes.extend_from_slice(b"CAT1");
    bytes.extend_from_slice(&1u16.to_le_bytes());
    bytes.extend_from_slice(&14u16.to_le_bytes());
    bytes.extend_from_slice(&71u32.to_le_bytes());
    bytes.extend_from_slice(&39u32.to_le_bytes());
    bytes.extend_from_slice(&[0; 16]);
    bytes.extend_from_slice(&14u16.to_be_bytes());
    bytes.push(1);
    bytes.extend_from_slice(&record.policy_id);
    bytes.push(1);
    bytes.extend_from_slice(&record.kind.to_be_bytes());
    bytes.push(1);
    bytes.extend_from_slice(&record.target_id);
    Ok(bytes)
}

/// Decode exactly one policy-target envelope and reject noncanonical bytes.
pub fn decode_policy_target(bytes: &[u8]) -> Result<PolicyTarget, InvalidCatalogRecord> {
    if bytes.len() != 71 {
        return Err(InvalidCatalogRecord);
    }
    let mut policy_id = [0; 16];
    policy_id.copy_from_slice(&bytes[35..51]);
    let mut target_id = [0; 16];
    target_id.copy_from_slice(&bytes[55..71]);
    let record = PolicyTarget {
        policy_id,
        kind: u16::from_be_bytes([bytes[52], bytes[53]]),
        target_id,
    };
    if encode_policy_target(&record)?.as_slice() != bytes {
        return Err(InvalidCatalogRecord);
    }
    Ok(record)
}
