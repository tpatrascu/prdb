use prdb_engine::row_record::{MAX_ROW_RECORD_BYTES, RowRecord, RowRecordError};

#[test]
fn row_record_has_canonical_header_and_round_trips_payload() {
    let record = RowRecord {
        row_version: 0x0807_0605_0403_0201,
        payload: vec![0, 0xff, 3],
    };
    let encoded = record.encode().unwrap();
    assert_eq!(&encoded[..8], b"RWR1\x01\x00\x00\x00");
    assert_eq!(&encoded[8..16], &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(&encoded[16..20], &[3, 0, 0, 0]);
    assert_eq!(RowRecord::decode(&encoded), Ok(record));
}

#[test]
fn row_record_rejects_malformed_versions_lengths_and_flags() {
    assert_eq!(
        RowRecord::decode(b"RWR"),
        Err(RowRecordError::InvalidLength)
    );
    let canonical = RowRecord {
        row_version: 1,
        payload: vec![7],
    }
    .encode()
    .unwrap();

    let mut bytes = canonical.clone();
    bytes[0] = b'X';
    assert_eq!(RowRecord::decode(&bytes), Err(RowRecordError::InvalidMagic));
    let mut bytes = canonical.clone();
    bytes[4] = 2;
    assert_eq!(
        RowRecord::decode(&bytes),
        Err(RowRecordError::UnsupportedVersion)
    );
    let mut bytes = canonical.clone();
    bytes[6] = 1;
    assert_eq!(RowRecord::decode(&bytes), Err(RowRecordError::NonzeroFlags));
    let mut bytes = canonical.clone();
    bytes[8..16].fill(0);
    assert_eq!(
        RowRecord::decode(&bytes),
        Err(RowRecordError::InvalidRowVersion)
    );
    let mut bytes = canonical.clone();
    bytes[16] = 2;
    assert_eq!(
        RowRecord::decode(&bytes),
        Err(RowRecordError::InvalidLength)
    );
    assert_eq!(
        RowRecord::decode(&canonical[..20]),
        Err(RowRecordError::InvalidLength)
    );
    assert_eq!(
        RowRecord {
            row_version: 0,
            payload: Vec::new(),
        }
        .encode(),
        Err(RowRecordError::InvalidRowVersion)
    );
}

#[test]
fn row_record_enforces_total_encoded_size_before_allocation() {
    let exact_payload = vec![9; MAX_ROW_RECORD_BYTES - 20];
    assert_eq!(
        RowRecord {
            row_version: 1,
            payload: exact_payload,
        }
        .encode()
        .unwrap()
        .len(),
        MAX_ROW_RECORD_BYTES
    );
    assert_eq!(
        RowRecord {
            row_version: 1,
            payload: vec![9; MAX_ROW_RECORD_BYTES - 19],
        }
        .encode(),
        Err(RowRecordError::TooLarge)
    );
}
