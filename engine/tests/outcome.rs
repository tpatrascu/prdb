use prdb_engine::outcome::{InvocationOutcome, OutcomeError, OutcomeStatus};

fn sample() -> InvocationOutcome {
    InvocationOutcome {
        status: OutcomeStatus::Committed,
        invocation_id: [0x11; 16],
        principal_id: [0x22; 16],
        commit_sequence: 0x0807_0605_0403_0201,
        retention_deadline_us: 0x0807_0605_0403_0201,
    }
}

#[test]
fn committed_outcome_is_canonical_and_round_trips() {
    let outcome = sample();
    let encoded = outcome.encode().unwrap();
    assert_eq!(&encoded[..8], b"OUT1\x01\x00\x01\x01");
    assert_eq!(&encoded[8..24], &[0x11; 16]);
    assert_eq!(&encoded[24..40], &[0x22; 16]);
    assert_eq!(&encoded[40..48], &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(&encoded[48..56], &[1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(InvocationOutcome::decode(&encoded), Ok(outcome));
}

#[test]
fn committed_outcome_rejects_unknown_status_ids_and_invalid_retention() {
    let canonical = sample().encode().unwrap();
    let mut bytes = canonical;
    bytes[0] = b'X';
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidMagic)
    );
    let mut bytes = canonical;
    bytes[6] = 3;
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidStatus)
    );
    let mut bytes = canonical;
    bytes[7] = 0;
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidRetentionState)
    );
    let mut bytes = canonical;
    bytes[8..24].fill(0);
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidId)
    );
    let mut bytes = canonical;
    bytes[40..48].fill(0);
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidCommitSequence)
    );
    let mut bytes = canonical;
    bytes[48..56].fill(0);
    assert_eq!(
        InvocationOutcome::decode(&bytes),
        Err(OutcomeError::InvalidRetentionDeadline)
    );
    assert_eq!(
        InvocationOutcome::decode(&canonical[..55]),
        Err(OutcomeError::InvalidLength)
    );
}

#[test]
fn aborted_outcome_uses_no_commit_sequence() {
    let aborted = InvocationOutcome {
        status: OutcomeStatus::Aborted,
        invocation_id: [0x11; 16],
        principal_id: [0x22; 16],
        commit_sequence: 0,
        retention_deadline_us: 100,
    };
    let encoded = aborted.encode().unwrap();
    assert_eq!(encoded[6], 2);
    assert_eq!(InvocationOutcome::decode(&encoded), Ok(aborted.clone()));
    let invalid = InvocationOutcome {
        commit_sequence: 1,
        ..aborted
    };
    assert_eq!(invalid.encode(), Err(OutcomeError::InvalidCommitSequence));
}
