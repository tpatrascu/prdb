use prdb_engine::metadata::{EngineMetadata, Lifecycle, MetadataError};

fn sample() -> EngineMetadata {
    EngineMetadata {
        database_id: [0x11; 16],
        catalog_version: 2,
        expression_version: 2,
        builtin_semantics_version: 3,
        commit_sequence: 0x0807_0605_0403_0201,
        lifecycle: Lifecycle::Initializing,
    }
}

#[test]
fn metadata_has_canonical_bytes_and_round_trips() {
    let metadata = sample();
    let encoded = metadata.encode().unwrap();
    assert_eq!(&encoded[..6], b"ENG1\x01\x00");
    assert_eq!(&encoded[6..22], &[0x11; 16]);
    assert_eq!(&encoded[22..34], &[2, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0]);
    assert_eq!(&encoded[34..42], &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(encoded[42], Lifecycle::Initializing as u8);
    assert_eq!(&encoded[43..46], &[0; 3]);
    assert_eq!(EngineMetadata::decode(&encoded), Ok(metadata));
}

#[test]
fn metadata_rejects_unknown_or_noncanonical_encodings() {
    let canonical = sample().encode().unwrap();
    assert_eq!(
        EngineMetadata::decode(&canonical[..45]),
        Err(MetadataError::InvalidLength)
    );
    assert_eq!(
        EngineMetadata::decode(&[canonical.as_slice(), &[0]].concat()),
        Err(MetadataError::InvalidLength)
    );

    let mut bytes = canonical;
    bytes[0] = b'X';
    assert_eq!(
        EngineMetadata::decode(&bytes),
        Err(MetadataError::InvalidMagic)
    );
    let mut bytes = canonical;
    bytes[4] = 2;
    assert_eq!(
        EngineMetadata::decode(&bytes),
        Err(MetadataError::UnsupportedVersion)
    );
    let mut bytes = canonical;
    bytes[42] = 3;
    assert_eq!(
        EngineMetadata::decode(&bytes),
        Err(MetadataError::InvalidLifecycle)
    );
    let mut bytes = canonical;
    bytes[45] = 1;
    assert_eq!(
        EngineMetadata::decode(&bytes),
        Err(MetadataError::NonzeroReserved)
    );
    let mut bytes = canonical;
    bytes[6..22].fill(0);
    assert_eq!(
        EngineMetadata::decode(&bytes),
        Err(MetadataError::InvalidDatabaseId)
    );
    let mut bytes = canonical;
    bytes[22..26].fill(0);
    assert_eq!(
        EngineMetadata::decode(&bytes),
        Err(MetadataError::InvalidRequiredVersion)
    );
}
