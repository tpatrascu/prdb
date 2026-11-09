use prdb_engine::ipc::{
    DispatchError, FrameError, IpcFrame, MAX_PAYLOAD_BYTES, Operation, ServiceRole,
    authorize_operation, encode_frame, read_frame, write_frame,
};
use std::io::{Cursor, Write};
use std::os::unix::net::UnixStream;
use std::thread;

#[test]
fn ipc_frame_matches_the_hand_checked_v1_fixture() {
    let frame = IpcFrame {
        operation: Operation::ResolveRelation as u16,
        request_id: 0x0102_0304_0506_0708,
        payload: b"ok".to_vec(),
    };
    assert_eq!(
        encode_frame(&frame).unwrap(),
        [
            b"PRDI".as_slice(),
            &[0, 1],
            &[0x11, 0x01],
            &[1, 2, 3, 4, 5, 6, 7, 8],
            &[0, 0, 0, 2],
            b"ok",
        ]
        .concat()
    );
}

#[test]
fn ipc_frame_reader_handles_split_and_coalesced_stream_writes() {
    let (mut sender, mut receiver) = UnixStream::pair().unwrap();
    let first = IpcFrame {
        operation: Operation::ResolveRelation as u16,
        request_id: 11,
        payload: b"fragmented".to_vec(),
    };
    let second = IpcFrame {
        operation: Operation::OpenScan as u16,
        request_id: 12,
        payload: b"coalesced-a".to_vec(),
    };
    let third = IpcFrame {
        operation: Operation::NextBatch as u16,
        request_id: 13,
        payload: b"coalesced-b".to_vec(),
    };
    let first_bytes = encode_frame(&first).unwrap();
    let coalesced = [
        encode_frame(&second).unwrap(),
        encode_frame(&third).unwrap(),
    ]
    .concat();
    let writer = thread::spawn(move || {
        sender.write_all(&first_bytes[..3]).unwrap();
        sender.write_all(&first_bytes[3..]).unwrap();
        sender.write_all(&coalesced).unwrap();
    });

    assert_eq!(read_frame(&mut receiver).unwrap(), Some(first));
    assert_eq!(read_frame(&mut receiver).unwrap(), Some(second));
    assert_eq!(read_frame(&mut receiver).unwrap(), Some(third));
    writer.join().unwrap();
}

#[test]
fn ipc_frame_rejects_invalid_headers_and_oversized_payload_before_reading_it() {
    let mut oversized = vec![0; 20];
    oversized[..4].copy_from_slice(b"PRDI");
    oversized[4..6].copy_from_slice(&1u16.to_be_bytes());
    oversized[6..8].copy_from_slice(&(Operation::ResolveRelation as u16).to_be_bytes());
    oversized[8..16].copy_from_slice(&1u64.to_be_bytes());
    oversized[16..20].copy_from_slice(&((MAX_PAYLOAD_BYTES as u32) + 1).to_be_bytes());
    assert!(matches!(
        read_frame(&mut Cursor::new(oversized)),
        Err(FrameError::PayloadTooLarge)
    ));

    let mut bad_magic = encode_frame(&IpcFrame {
        operation: Operation::ResolveRelation as u16,
        request_id: 1,
        payload: Vec::new(),
    })
    .unwrap();
    bad_magic[0] = b'X';
    assert!(matches!(
        read_frame(&mut Cursor::new(bad_magic)),
        Err(FrameError::InvalidMagic)
    ));

    let mut unsupported_version = encode_frame(&IpcFrame {
        operation: Operation::ResolveRelation as u16,
        request_id: 1,
        payload: Vec::new(),
    })
    .unwrap();
    unsupported_version[5] = 2;
    assert!(matches!(
        read_frame(&mut Cursor::new(unsupported_version)),
        Err(FrameError::UnsupportedVersion(2))
    ));
}

#[test]
fn ipc_frame_rejects_zero_ids_and_truncated_frames() {
    assert!(matches!(
        encode_frame(&IpcFrame {
            operation: 0,
            request_id: 1,
            payload: Vec::new(),
        }),
        Err(FrameError::InvalidOperation)
    ));
    assert!(matches!(
        encode_frame(&IpcFrame {
            operation: Operation::ResolveRelation as u16,
            request_id: 0,
            payload: Vec::new(),
        }),
        Err(FrameError::InvalidRequestId)
    ));
    assert!(matches!(
        read_frame(&mut Cursor::new(b"PRD")),
        Err(FrameError::TruncatedFrame)
    ));

    let mut encoded = encode_frame(&IpcFrame {
        operation: Operation::ResolveRelation as u16,
        request_id: 1,
        payload: b"body".to_vec(),
    })
    .unwrap();
    encoded.pop();
    assert!(matches!(
        read_frame(&mut Cursor::new(encoded)),
        Err(FrameError::TruncatedFrame)
    ));
}

#[test]
fn ipc_frame_writer_enforces_payload_limit() {
    let frame = IpcFrame {
        operation: Operation::ResolveRelation as u16,
        request_id: 1,
        payload: vec![0; MAX_PAYLOAD_BYTES + 1],
    };
    let mut output = Vec::new();
    assert!(matches!(
        write_frame(&mut output, &frame),
        Err(FrameError::PayloadTooLarge)
    ));
    assert!(output.is_empty());
}

#[test]
fn ipc_frame_error_statuses_match_the_shared_cross_language_fixture() {
    let cases = [
        (FrameError::InvalidMagic, 2),
        (FrameError::UnsupportedVersion(2), 3),
        (FrameError::InvalidOperation, 4),
        (FrameError::UnknownOperation(0x7fff), 5),
        (FrameError::InvalidRequestId, 6),
        (FrameError::PayloadTooLarge, 7),
        (FrameError::TruncatedFrame, 8),
    ];
    for (error, expected_code) in cases {
        assert_eq!(error.status_code(), Some(expected_code));
    }
    assert_eq!(
        FrameError::Io(std::io::ErrorKind::Other.into()).status_code(),
        None
    );
}

#[test]
fn ipc_operation_codes_are_fixed_and_unknown_codes_fail_closed() {
    assert_eq!(Operation::BootstrapDeliverCredential as u16, 0x0101);
    assert_eq!(Operation::BootstrapAcknowledgeDelivery as u16, 0x0102);
    assert_eq!(Operation::ResolveRelation as u16, 0x1101);
    assert_eq!(Operation::EstimateScan as u16, 0x1202);
    assert_eq!(Operation::CommitAdministrativeDdl as u16, 0x1501);
    assert_eq!(
        Operation::from_code(Operation::StageUpdate as u16),
        Ok(Operation::StageUpdate)
    );
    assert_eq!(
        Operation::from_code(0x7fff),
        Err(DispatchError::UnknownOperation(0x7fff))
    );

    assert!(matches!(
        encode_frame(&IpcFrame {
            operation: 0x7fff,
            request_id: 1,
            payload: Vec::new(),
        }),
        Err(FrameError::UnknownOperation(0x7fff))
    ));

    let mut unknown_header = encode_frame(&IpcFrame {
        operation: Operation::ResolveRelation as u16,
        request_id: 1,
        payload: Vec::new(),
    })
    .unwrap();
    unknown_header[6..8].copy_from_slice(&0x7fffu16.to_be_bytes());
    assert!(matches!(
        read_frame(&mut Cursor::new(unknown_header)),
        Err(FrameError::UnknownOperation(0x7fff))
    ));
}

#[test]
fn ipc_operation_allowlists_separate_executor_planner_and_control_roles() {
    assert_eq!(
        authorize_operation(
            ServiceRole::Bootstrap,
            Operation::BootstrapDeliverCredential
        ),
        Ok(())
    );
    assert_eq!(
        authorize_operation(ServiceRole::Executor, Operation::StageUpdate),
        Ok(())
    );
    assert_eq!(
        authorize_operation(ServiceRole::Planner, Operation::EstimateScan),
        Ok(())
    );
    assert_eq!(
        authorize_operation(ServiceRole::Supervisor, Operation::FinalizeTransaction),
        Ok(())
    );
    assert_eq!(
        authorize_operation(
            ServiceRole::Gateway,
            Operation::SubmitAdministrativeOperation
        ),
        Ok(())
    );
    assert_eq!(
        authorize_operation(
            ServiceRole::AdminControl,
            Operation::CommitAdministrativeDdl
        ),
        Ok(())
    );

    for (role, operation) in [
        (ServiceRole::Executor, Operation::EstimateScan),
        (ServiceRole::Executor, Operation::BootstrapDeliverCredential),
        (ServiceRole::Bootstrap, Operation::ResolveRelation),
        (ServiceRole::Executor, Operation::FinalizeTransaction),
        (ServiceRole::Executor, Operation::CommitAdministrativeDdl),
        (ServiceRole::Planner, Operation::OpenScan),
        (ServiceRole::Planner, Operation::FinalizeTransaction),
        (ServiceRole::Supervisor, Operation::StageUpdate),
        (ServiceRole::Supervisor, Operation::EstimateScan),
        (ServiceRole::Gateway, Operation::CommitAdministrativeDdl),
        (ServiceRole::Gateway, Operation::FinalizeTransaction),
        (ServiceRole::AdminControl, Operation::StageInsert),
        (ServiceRole::AdminControl, Operation::FinalizeTransaction),
    ] {
        assert_eq!(
            authorize_operation(role, operation),
            Err(DispatchError::OperationNotAllowed { role, operation }),
            "unexpected authorization for {role:?} and {operation:?}"
        );
    }
}
