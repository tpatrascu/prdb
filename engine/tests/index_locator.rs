use prdb_engine::index_locator::{IndexLocator, LOCATOR_LENGTH, LocatorError};

#[test]
fn locator_has_a_fixed_versioned_encoding() {
    let locator = IndexLocator {
        row_id: [0xa5; 16],
        row_version: 0x0102_0304_0506_0708,
    };
    let encoded = locator.encode();
    assert_eq!(encoded.len(), LOCATOR_LENGTH);
    assert_eq!(&encoded[..4], b"ILC1");
    assert_eq!(&encoded[4..6], &1u16.to_le_bytes());
    assert_eq!(&encoded[6..8], &[0, 0]);
    assert_eq!(&encoded[8..24], &[0xa5; 16]);
    assert_eq!(&encoded[24..], &0x0102_0304_0506_0708u64.to_le_bytes());
    assert_eq!(IndexLocator::decode(&encoded), Ok(locator));
}

#[test]
fn locator_decoder_rejects_unknown_or_malformed_values() {
    let valid = IndexLocator {
        row_id: [1; 16],
        row_version: 1,
    }
    .encode();
    assert_eq!(
        IndexLocator::decode(&valid[..31]),
        Err(LocatorError::InvalidLength)
    );
    let mut bad = valid;
    bad[0] = b'X';
    assert_eq!(IndexLocator::decode(&bad), Err(LocatorError::InvalidMagic));
    let mut bad = valid;
    bad[4] = 2;
    assert_eq!(
        IndexLocator::decode(&bad),
        Err(LocatorError::UnsupportedVersion)
    );
    let mut bad = valid;
    bad[6] = 1;
    assert_eq!(IndexLocator::decode(&bad), Err(LocatorError::ReservedBytes));
    let mut bad = valid;
    bad[24..].fill(0);
    assert_eq!(
        IndexLocator::decode(&bad),
        Err(LocatorError::InvalidRowVersion)
    );
}

#[test]
fn locator_must_match_the_row_uuid_in_the_index_key_suffix() {
    let row_id = [7; 16];
    let locator = IndexLocator {
        row_id,
        row_version: 8,
    }
    .encode();
    let mut key = vec![1; 19];
    key.extend_from_slice(&row_id);
    assert!(IndexLocator::decode_for_key(&key, &locator).is_ok());

    key[19] = 6;
    assert_eq!(
        IndexLocator::decode_for_key(&key, &locator),
        Err(LocatorError::RowIdMismatch)
    );
    assert_eq!(
        IndexLocator::decode_for_key(&key[..31], &locator),
        Err(LocatorError::InvalidIndexKey)
    );
}
