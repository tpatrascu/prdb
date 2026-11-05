use prdb_engine::audit::{AuditAction, AuditError, AuditEvent, AuditResult};

fn event() -> AuditEvent {
    AuditEvent {
        event_id: [1; 16],
        actor_id: Some([2; 16]),
        correlation_id: [3; 16],
        occurred_at_us: 1_700_000_000_123_456,
        action: AuditAction::Ddl,
        result: AuditResult::Succeeded,
        target_ids: vec![[4; 16], [5; 16]],
    }
}

#[test]
fn audit_event_matches_the_aud1_fixture_and_round_trips() {
    let encoded = event().encode().unwrap();
    let mut expected = b"AUD1".to_vec();
    expected.extend_from_slice(&1u16.to_le_bytes());
    expected.extend_from_slice(&1u16.to_le_bytes());
    expected.extend_from_slice(&1u16.to_le_bytes());
    expected.extend_from_slice(&2u16.to_le_bytes());
    expected.extend_from_slice(&[1; 16]);
    expected.extend_from_slice(&[2; 16]);
    expected.extend_from_slice(&[3; 16]);
    expected.extend_from_slice(&1_700_000_000_123_456i64.to_le_bytes());
    expected.extend_from_slice(&[4; 16]);
    expected.extend_from_slice(&[5; 16]);
    assert_eq!(encoded, expected);
    assert_eq!(AuditEvent::decode(&encoded), Ok(event()));
}

#[test]
fn audit_event_rejects_malformed_ids_codes_timestamps_and_targets() {
    let encoded = event().encode().unwrap();
    let mut invalid = encoded.clone();
    invalid[0] = b'X';
    assert_eq!(AuditEvent::decode(&invalid), Err(AuditError::InvalidMagic));
    let mut invalid = encoded.clone();
    invalid[6..8].copy_from_slice(&99u16.to_le_bytes());
    assert_eq!(AuditEvent::decode(&invalid), Err(AuditError::InvalidAction));
    let mut invalid = encoded.clone();
    invalid[12..28].fill(0);
    assert_eq!(AuditEvent::decode(&invalid), Err(AuditError::InvalidId));
    let mut invalid = encoded.clone();
    invalid[60..68].fill(0);
    assert_eq!(
        AuditEvent::decode(&invalid),
        Err(AuditError::InvalidTimestamp)
    );
    let mut invalid = encoded.clone();
    invalid[68..84].copy_from_slice(&[5; 16]);
    assert_eq!(
        AuditEvent::decode(&invalid),
        Err(AuditError::InvalidTargetList)
    );
    assert_eq!(
        AuditEvent::decode(&encoded[..encoded.len() - 1]),
        Err(AuditError::InvalidLength)
    );
}

#[test]
fn unauthenticated_audit_events_use_zero_actor_bytes() {
    let mut event = event();
    event.actor_id = None;
    event.action = AuditAction::AuthenticationFailure;
    event.result = AuditResult::Denied;
    let encoded = event.encode().unwrap();
    assert_eq!(&encoded[28..44], &[0; 16]);
    assert_eq!(AuditEvent::decode(&encoded), Ok(event));
}
