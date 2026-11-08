//! Canonical client-requested capability scope for submitted invocations.

use crate::authorization::{AccessTarget, Operation};
use std::collections::HashSet;

pub const REQUESTED_SCOPE_V1_MAGIC: &[u8; 4] = b"PSC1";
pub const MAX_REQUESTED_SCOPE_CAPABILITIES: usize = 200;

pub type RequestedScope = HashSet<(Operation, AccessTarget)>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestedScopeError {
    InvalidHeader,
    InvalidCount,
    InvalidRecord,
    NonCanonicalOrder,
}

/// Decode PSC1: magic, big-endian u16 record count, then strictly sorted
/// records. A record is operation:u8, target-kind:u8, and one or two 16-byte
/// IDs. Only schema USAGE and table/column data operations are representable.
pub fn decode_requested_scope_v1(bytes: &[u8]) -> Result<RequestedScope, RequestedScopeError> {
    if bytes.len() < 6 || &bytes[..4] != REQUESTED_SCOPE_V1_MAGIC {
        return Err(RequestedScopeError::InvalidHeader);
    }
    let count = u16::from_be_bytes([bytes[4], bytes[5]]) as usize;
    if count == 0 || count > MAX_REQUESTED_SCOPE_CAPABILITIES {
        return Err(RequestedScopeError::InvalidCount);
    }
    let mut cursor = 6usize;
    let mut previous_record: Option<Vec<u8>> = None;
    let mut scope = HashSet::with_capacity(count);
    for _ in 0..count {
        let header_end = cursor
            .checked_add(2)
            .ok_or(RequestedScopeError::InvalidRecord)?;
        let header = bytes
            .get(cursor..header_end)
            .ok_or(RequestedScopeError::InvalidRecord)?;
        let operation = decode_operation(header[0]).ok_or(RequestedScopeError::InvalidRecord)?;
        let target_kind = header[1];
        let id_count = match target_kind {
            1 | 2 => 1,
            3 => 2,
            _ => return Err(RequestedScopeError::InvalidRecord),
        };
        let record_end = header_end
            .checked_add(id_count * 16)
            .ok_or(RequestedScopeError::InvalidRecord)?;
        let record = bytes
            .get(cursor..record_end)
            .ok_or(RequestedScopeError::InvalidRecord)?;
        if previous_record
            .as_ref()
            .is_some_and(|previous| previous.as_slice() >= record)
        {
            return Err(RequestedScopeError::NonCanonicalOrder);
        }
        let first_id: [u8; 16] = record[2..18]
            .try_into()
            .map_err(|_| RequestedScopeError::InvalidRecord)?;
        if first_id == [0; 16] {
            return Err(RequestedScopeError::InvalidRecord);
        }
        let target = match target_kind {
            1 if operation == Operation::Usage => AccessTarget::Schema(first_id),
            2 if operation != Operation::Usage => AccessTarget::Table(first_id),
            3 if operation != Operation::Usage => {
                let second_id: [u8; 16] = record[18..34]
                    .try_into()
                    .map_err(|_| RequestedScopeError::InvalidRecord)?;
                if second_id == [0; 16] {
                    return Err(RequestedScopeError::InvalidRecord);
                }
                AccessTarget::Column {
                    table_id: first_id,
                    column_id: second_id,
                }
            }
            _ => return Err(RequestedScopeError::InvalidRecord),
        };
        if !scope.insert((operation, target)) {
            return Err(RequestedScopeError::NonCanonicalOrder);
        }
        previous_record = Some(record.to_vec());
        cursor = record_end;
    }
    if cursor != bytes.len() {
        return Err(RequestedScopeError::InvalidRecord);
    }
    Ok(scope)
}

fn decode_operation(value: u8) -> Option<Operation> {
    match value {
        1 => Some(Operation::Usage),
        2 => Some(Operation::Select),
        3 => Some(Operation::Insert),
        4 => Some(Operation::Update),
        5 => Some(Operation::Delete),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{AccessTarget, Operation, RequestedScopeError, decode_requested_scope_v1};

    #[test]
    fn requested_scope_v1_decodes_sorted_schema_table_and_column_capabilities() {
        let mut bytes = b"PSC1\0\x03".to_vec();
        bytes.extend([1, 1]);
        bytes.extend([1; 16]);
        bytes.extend([2, 2]);
        bytes.extend([2; 16]);
        bytes.extend([2, 3]);
        bytes.extend([2; 16]);
        bytes.extend([3; 16]);

        let scope = decode_requested_scope_v1(&bytes).unwrap();
        assert_eq!(scope.len(), 3);
        assert!(scope.contains(&(Operation::Usage, AccessTarget::Schema([1; 16]))));
        assert!(scope.contains(&(Operation::Select, AccessTarget::Table([2; 16]))));
        assert!(scope.contains(&(
            Operation::Select,
            AccessTarget::Column {
                table_id: [2; 16],
                column_id: [3; 16],
            }
        )));
    }

    #[test]
    fn requested_scope_v1_rejects_noncanonical_or_unsafe_records() {
        let mut reversed = b"PSC1\0\x02".to_vec();
        reversed.extend([2, 2]);
        reversed.extend([2; 16]);
        reversed.extend([1, 1]);
        reversed.extend([1; 16]);
        assert_eq!(
            decode_requested_scope_v1(&reversed),
            Err(RequestedScopeError::NonCanonicalOrder)
        );

        let mut usage_table = b"PSC1\0\x01".to_vec();
        usage_table.extend([1, 2]);
        usage_table.extend([1; 16]);
        assert_eq!(
            decode_requested_scope_v1(&usage_table),
            Err(RequestedScopeError::InvalidRecord)
        );

        let mut duplicate = b"PSC1\0\x02".to_vec();
        duplicate.extend([2, 2]);
        duplicate.extend([1; 16]);
        duplicate.extend([2, 2]);
        duplicate.extend([1; 16]);
        assert_eq!(
            decode_requested_scope_v1(&duplicate),
            Err(RequestedScopeError::NonCanonicalOrder)
        );
    }

    #[test]
    fn requested_scope_v1_rejects_trailing_truncated_zero_and_oversized_inputs() {
        assert_eq!(
            decode_requested_scope_v1(b"PSC1\0\0"),
            Err(RequestedScopeError::InvalidCount)
        );
        assert_eq!(
            decode_requested_scope_v1(b"PSC1\0\x01\x02\x02"),
            Err(RequestedScopeError::InvalidRecord)
        );
        let mut zero_id = b"PSC1\0\x01".to_vec();
        zero_id.extend([2, 2]);
        zero_id.extend([0; 16]);
        assert_eq!(
            decode_requested_scope_v1(&zero_id),
            Err(RequestedScopeError::InvalidRecord)
        );
        let oversized_count = b"PSC1\0\xc9".to_vec();
        assert_eq!(
            decode_requested_scope_v1(&oversized_count),
            Err(RequestedScopeError::InvalidCount)
        );
        let mut trailing = b"PSC1\0\x01\x02\x02".to_vec();
        trailing.extend([1; 16]);
        trailing.push(0);
        assert_eq!(
            decode_requested_scope_v1(&trailing),
            Err(RequestedScopeError::InvalidRecord)
        );
    }
}
