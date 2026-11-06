use prdb_engine::ipc::{
    FrameError, IpcFrame, MAX_PAYLOAD_BYTES, encode_frame, read_frame, write_frame,
};
use std::io::{Cursor, Write};
use std::os::unix::net::UnixStream;
use std::thread;

#[test]
fn ipc_frame_matches_the_hand_checked_v1_fixture() {
    let frame = IpcFrame {
        operation: 0x1234,
        request_id: 0x0102_0304_0506_0708,
        payload: b"ok".to_vec(),
    };
    assert_eq!(
        encode_frame(&frame).unwrap(),
        [
            b"PRDI".as_slice(),
            &[0, 1],
            &[0x12, 0x34],
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
        operation: 1,
        request_id: 11,
        payload: b"fragmented".to_vec(),
    };
    let second = IpcFrame {
        operation: 2,
        request_id: 12,
        payload: b"coalesced-a".to_vec(),
    };
    let third = IpcFrame {
        operation: 3,
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
    oversized[6..8].copy_from_slice(&1u16.to_be_bytes());
    oversized[8..16].copy_from_slice(&1u64.to_be_bytes());
    oversized[16..20].copy_from_slice(&((MAX_PAYLOAD_BYTES as u32) + 1).to_be_bytes());
    assert!(matches!(
        read_frame(&mut Cursor::new(oversized)),
        Err(FrameError::PayloadTooLarge)
    ));

    let mut bad_magic = encode_frame(&IpcFrame {
        operation: 1,
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
        operation: 1,
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
            operation: 1,
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
        operation: 1,
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
        operation: 1,
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
