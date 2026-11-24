use prdb_engine::authorization::SanitizedCell;
use prdb_engine::executor_ipc::{
    MutationAcknowledgement, OpenScanRequest, OpenScanResponse, ResolveRelationRequest,
    ResolveRelationResponse, ScanIndexProbe, ScanIndexRangesRequest, ScanIndexRangesResponse,
    StageDeleteRequest, StageInsertRequest, StageInsertResponse, StageUpdateRequest,
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
fn batched_index_probe_payloads_preserve_probe_and_result_group_order() {
    let request = ScanIndexRangesRequest {
        relation: ResolvedRelationHandle::from_bytes(&handle(6)).unwrap(),
        index_id: handle(7),
        probes: vec![
            ScanIndexProbe {
                values: vec![ScalarValue::Integer(11)],
            },
            ScanIndexProbe {
                values: vec![ScalarValue::Text("region".into()), ScalarValue::BigInt(9)],
            },
        ],
        column_ids: vec![handle(9), handle(10)],
    };
    assert_eq!(
        ScanIndexRangesRequest::decode(&request.encode().unwrap()).unwrap(),
        request
    );

    let response = ScanIndexRangesResponse::Groups(vec![
        vec![vec![SanitizedCell::Value(ScalarValue::Integer(11))]],
        vec![
            vec![SanitizedCell::Null],
            vec![
                SanitizedCell::Redacted,
                SanitizedCell::Value(ScalarValue::BigInt(9)),
            ],
        ],
    ]);
    let encoded = response.encode().unwrap();
    assert_eq!(ScanIndexRangesResponse::decode(&encoded).unwrap(), response);
    assert_eq!(
        ScanIndexRangesResponse::decode(&ScanIndexRangesResponse::Unavailable.encode().unwrap())
            .unwrap(),
        ScanIndexRangesResponse::Unavailable
    );
}

#[test]
fn batched_index_probe_payloads_reject_bad_counts_trailing_data_and_partial_groups() {
    let request = ScanIndexRangesRequest {
        relation: ResolvedRelationHandle::from_bytes(&handle(6)).unwrap(),
        index_id: handle(7),
        probes: vec![ScanIndexProbe {
            values: vec![ScalarValue::Integer(11)],
        }],
        column_ids: vec![handle(9)],
    };
    let mut malformed = request.encode().unwrap();
    malformed[34..36].copy_from_slice(&129u16.to_be_bytes());
    assert!(ScanIndexRangesRequest::decode(&malformed).is_err());

    let mut trailing = request.encode().unwrap();
    trailing.push(0);
    assert!(ScanIndexRangesRequest::decode(&trailing).is_err());

    let mut malformed_response = ScanIndexRangesResponse::Groups(vec![vec![]])
        .encode()
        .unwrap();
    malformed_response[2..4].copy_from_slice(&2u16.to_be_bytes());
    assert!(ScanIndexRangesResponse::decode(&malformed_response).is_err());

    let over_limit = ScanIndexRangesResponse::Groups(vec![vec![vec![]; 1_025]])
        .encode()
        .unwrap();
    assert_eq!(over_limit, vec![1, 0]);
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
fn executor_open_scan_request_round_trips_generated_column_ids() {
    let request = OpenScanRequest {
        relation: ResolvedRelationHandle::from_bytes(&handle(6)).unwrap(),
        column_ids: vec![[0x31; 16], [0x42; 16]],
    };
    let payload = request.encode().unwrap();
    assert_eq!(
        &payload[..20],
        &[&[0, 1][..], &handle(6), &[0, 2][..]].concat()
    );
    assert_eq!(&payload[20..36], &[0x31; 16]);
    assert_eq!(&payload[36..], &[0x42; 16]);
    assert_eq!(OpenScanRequest::decode(&payload).unwrap(), request);
    assert!(OpenScanRequest::decode(&payload[..payload.len() - 1]).is_err());
    let mut trailing = payload;
    trailing.push(0);
    assert!(OpenScanRequest::decode(&trailing).is_err());
}

#[test]
fn executor_resolve_relation_request_uses_generated_utf8_tails() {
    let request = ResolveRelationRequest {
        schema_name: "app".to_owned(),
        relation_name: "items".to_owned(),
    };
    let payload = request.encode().unwrap();
    assert_eq!(
        payload,
        [
            0, 1, 0, 3, 0, 5, b'a', b'p', b'p', b'i', b't', b'e', b'm', b's'
        ]
    );
    assert_eq!(ResolveRelationRequest::decode(&payload).unwrap(), request);
    let mut invalid_utf8 = payload.clone();
    invalid_utf8[6] = 0xff;
    assert!(ResolveRelationRequest::decode(&invalid_utf8).is_err());
    let mut trailing = payload;
    trailing.push(0);
    assert!(ResolveRelationRequest::decode(&trailing).is_err());
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
    let mut empty_relation_name = payload.clone();
    empty_relation_name[35..37].copy_from_slice(&[0, 0]);
    assert!(ResolveRelationResponse::decode(&empty_relation_name).is_err());
    let mut invalid_column_name = payload.clone();
    invalid_column_name[72] = 0xff;
    assert!(ResolveRelationResponse::decode(&invalid_column_name).is_err());
    let mut invalid_nullable_flag = payload.clone();
    invalid_nullable_flag[64] = 2;
    assert!(ResolveRelationResponse::decode(&invalid_nullable_flag).is_err());
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
