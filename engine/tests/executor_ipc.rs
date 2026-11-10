use prdb_engine::executor_ipc::{
    MutationAcknowledgement, OpenScanResponse, ResolveRelationResponse, StageDeleteRequest,
    StageInsertRequest, StageInsertResponse, StageUpdateRequest,
};
use prdb_engine::metadata_discovery::{
    AuthorizedRelationDescription, EXECUTOR_SCAN_CURSOR_TOKEN_KIND, ExecutorRowHandle,
    ExecutorScanCursor, ResolvedRelationHandle, VisibleColumn,
};
use prdb_engine::transaction::InsertInput;
use prdb_engine::tuple::{FieldPatch, ScalarType, ScalarValue};

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
            InsertInput::Value(Some(ScalarValue::Integer(-42))),
            InsertInput::Value(None),
            InsertInput::Default,
            InsertInput::Value(Some(ScalarValue::Bytea(vec![0, 1, 255]))),
        ],
    };
    let insert = StageInsertRequest::decode(&insert.encode().unwrap()).unwrap();
    assert_eq!(
        insert.values[0],
        InsertInput::Value(Some(ScalarValue::Integer(-42)))
    );
    assert_eq!(insert.values[1], InsertInput::Value(None));
    assert_eq!(insert.values[2], InsertInput::Default);
    assert_eq!(
        insert.values[3],
        InsertInput::Value(Some(ScalarValue::Bytea(vec![0, 1, 255])))
    );

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
fn executor_open_scan_response_round_trips_generated_cursor_handle() {
    let mut cursor_bytes = [0x5a; 16];
    cursor_bytes[0] = EXECUTOR_SCAN_CURSOR_TOKEN_KIND;
    let response = OpenScanResponse {
        cursor: ExecutorScanCursor::from_bytes(&cursor_bytes).unwrap(),
    };
    let payload = response.encode();
    assert_eq!(&payload[..2], &[0x00, 0x01]);
    assert_eq!(&payload[2..], &cursor_bytes);
    assert_eq!(OpenScanResponse::decode(&payload).unwrap(), response);
    assert!(OpenScanResponse::decode(&payload[..payload.len() - 1]).is_err());
}

#[test]
fn executor_relation_response_uses_generated_prefix_codecs() {
    let relation_handle = ResolvedRelationHandle::from_bytes(&handle(6)).unwrap();
    let response = ResolveRelationResponse::Resolved {
        handle: relation_handle,
        description: AuthorizedRelationDescription {
            relation_id: [0x31; 16],
            name: "items".to_owned(),
            columns: vec![VisibleColumn {
                column_id: [0x42; 16],
                name: "id".to_owned(),
                ordinal: 0,
                scalar_type: ScalarType::Integer,
                nullable: false,
            }],
        },
    };
    let payload = response.encode().unwrap();
    assert_eq!(
        &payload[..35],
        &[&[0, 1, 0][..], &[0x31; 16], &handle(6)].concat()
    );
    assert_eq!(ResolveRelationResponse::decode(&payload).unwrap(), response);

    let unavailable = ResolveRelationResponse::Unavailable.encode().unwrap();
    assert_eq!(unavailable, [0, 1, 1]);
    assert_eq!(
        ResolveRelationResponse::decode(&unavailable).unwrap(),
        ResolveRelationResponse::Unavailable
    );
    for malformed in [&[0, 1, 1, 0][..], &[0, 2, 1][..], &[0, 1, 2][..]] {
        assert!(ResolveRelationResponse::decode(malformed).is_err());
    }
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
        values: vec![InsertInput::Value(Some(ScalarValue::Boolean(true)))],
    };
    let mut payload = insert.encode().unwrap();
    payload[20] = 3; // Unknown cell states are invalid; REDACTED is response-only.
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
