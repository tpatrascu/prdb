use prdb_engine::executor_ipc::{
    MutationAcknowledgement, StageDeleteRequest, StageInsertRequest, StageInsertResponse,
    StageUpdateRequest,
};
use prdb_engine::metadata_discovery::{ExecutorRowHandle, ResolvedRelationHandle};
use prdb_engine::tuple::{FieldPatch, ScalarValue};

fn handle(kind: u8) -> [u8; 16] {
    let mut bytes = [0x5a; 16];
    bytes[0] = kind;
    bytes
}

#[test]
fn executor_mutation_payloads_round_trip_canonical_values_and_handles() {
    let relation = ResolvedRelationHandle::from_bytes(&handle(6)).unwrap();
    let insert = StageInsertRequest {
        relation,
        values: vec![
            Some(ScalarValue::Integer(-42)),
            None,
            Some(ScalarValue::Text("value".to_owned())),
            Some(ScalarValue::Bytea(vec![0, 1, 255])),
        ],
    };
    let insert = StageInsertRequest::decode(&insert.encode().unwrap()).unwrap();
    assert_eq!(insert.values[0], Some(ScalarValue::Integer(-42)));
    assert_eq!(insert.values[1], None);
    assert_eq!(
        insert.values[2],
        Some(ScalarValue::Text("value".to_owned()))
    );
    assert_eq!(insert.values[3], Some(ScalarValue::Bytea(vec![0, 1, 255])));

    let row = ExecutorRowHandle::from_bytes(&handle(8)).unwrap();
    let update = StageUpdateRequest {
        handle: row.clone(),
        patches: vec![
            FieldPatch {
                ordinal: 0,
                value: Some(ScalarValue::BigInt(i64::MIN)),
            },
            FieldPatch {
                ordinal: 3,
                value: None,
            },
        ],
    };
    assert_eq!(
        StageUpdateRequest::decode(&update.encode().unwrap()).unwrap(),
        update
    );

    let delete = StageDeleteRequest {
        handle: row.clone(),
    };
    assert_eq!(
        StageDeleteRequest::decode(&delete.encode()).unwrap(),
        delete
    );
    let response = StageInsertResponse { handle: row };
    assert_eq!(
        StageInsertResponse::decode(&response.encode()).unwrap(),
        response
    );
    assert_eq!(
        MutationAcknowledgement::decode(&MutationAcknowledgement.encode()).unwrap(),
        MutationAcknowledgement
    );
}

#[test]
fn executor_mutation_payloads_reject_duplicate_or_redacted_fields() {
    let row = ExecutorRowHandle::from_bytes(&handle(8)).unwrap();
    let duplicate = StageUpdateRequest {
        handle: row.clone(),
        patches: vec![
            FieldPatch {
                ordinal: 1,
                value: None,
            },
            FieldPatch {
                ordinal: 1,
                value: None,
            },
        ],
    };
    assert!(duplicate.encode().is_err());

    let relation = ResolvedRelationHandle::from_bytes(&handle(6)).unwrap();
    let insert = StageInsertRequest {
        relation,
        values: vec![Some(ScalarValue::Boolean(true))],
    };
    let mut payload = insert.encode().unwrap();
    payload[20] = 2; // REDACTED is a response-only state.
    assert!(StageInsertRequest::decode(&payload).is_err());

    let mut patch = StageUpdateRequest {
        handle: row,
        patches: vec![FieldPatch {
            ordinal: 0,
            value: Some(ScalarValue::Boolean(true)),
        }],
    }
    .encode()
    .unwrap();
    patch[24] = 2;
    assert!(StageUpdateRequest::decode(&patch).is_err());
}
