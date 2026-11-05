use prdb_engine::overlay::{Overlay, OverlayError, ResolvedRow, RowId, StagedRow};

fn id(n: u8) -> RowId {
    RowId {
        table: [1; 16],
        row: [n; 16],
    }
}

#[test]
fn staged_rows_are_private_and_replacement_is_accounted() {
    let mut a = Overlay::new(2, 4);
    let b = Overlay::new(2, 4);
    a.put(id(1), b"abc").unwrap();
    assert_eq!(a.get(&id(1)).unwrap(), Some(StagedRow::Value(&b"abc"[..])));
    assert_eq!(b.get(&id(1)).unwrap(), None);
    a.put(id(1), b"1234").unwrap();
    a.delete(id(2)).unwrap();
    assert_eq!(a.get(&id(1)).unwrap(), Some(StagedRow::Value(&b"1234"[..])));
    assert_eq!(a.get(&id(2)).unwrap(), Some(StagedRow::Delete));
    a.delete(id(1)).unwrap();
    a.put(id(1), b"1234").unwrap();
}

#[test]
fn exceeding_limits_aborts_and_clears_the_whole_overlay() {
    let mut a = Overlay::new(2, 4);
    a.put(id(1), b"1234").unwrap();
    assert_eq!(a.put(id(2), b"x"), Err(OverlayError::LimitExceeded));
    assert_eq!(a.get(&id(1)), Err(OverlayError::Aborted));
    assert_eq!(a.delete(id(1)), Err(OverlayError::Aborted));
    let mut b = Overlay::new(1, 4);
    b.delete(id(1)).unwrap();
    assert_eq!(b.delete(id(2)), Err(OverlayError::LimitExceeded));
    assert_eq!(b.get(&id(1)), Err(OverlayError::Aborted));
}

#[test]
fn staged_input_is_owned_and_explicit_abort_is_terminal() {
    let mut overlay = Overlay::new(1, 3);
    let mut input = b"abc".to_vec();
    overlay.put(id(1), &input).unwrap();
    input.fill(b'x');
    assert_eq!(
        overlay.get(&id(1)).unwrap(),
        Some(StagedRow::Value(&b"abc"[..]))
    );
    overlay.abort();
    assert_eq!(overlay.get(&id(1)), Err(OverlayError::Aborted));
    assert_eq!(overlay.put(id(1), b""), Err(OverlayError::Aborted));
}

#[test]
fn zero_limits_and_replacement_at_limit_are_enforced() {
    let mut no_rows = Overlay::new(0, 5);
    assert_eq!(no_rows.put(id(1), b""), Err(OverlayError::LimitExceeded));
    let mut no_bytes = Overlay::new(1, 0);
    no_bytes.put(id(1), b"").unwrap();
    no_bytes.delete(id(1)).unwrap();
    assert_eq!(no_bytes.put(id(1), b"x"), Err(OverlayError::LimitExceeded));
}

#[test]
fn reads_resolve_overlay_before_snapshot_without_changing_snapshot() {
    let mut overlay = Overlay::new(3, 12);
    let snapshot = |row: &RowId| match row.row[0] {
        1 => Some(&b"old"[..]),
        2 => Some(&b"gone"[..]),
        _ => None,
    };

    {
        assert_eq!(
            overlay.read(&id(1), snapshot).unwrap(),
            Some(ResolvedRow::SnapshotValue(&b"old"[..]))
        );
    }
    overlay.put(id(1), b"new").unwrap();
    overlay.delete(id(2)).unwrap();
    overlay.put(id(3), b"insert").unwrap();

    {
        assert_eq!(
            overlay.read(&id(1), snapshot).unwrap(),
            Some(ResolvedRow::OverlayValue(&b"new"[..]))
        );
        assert_eq!(overlay.read(&id(2), snapshot).unwrap(), None);
        assert_eq!(
            overlay.read(&id(3), snapshot).unwrap(),
            Some(ResolvedRow::OverlayValue(&b"insert"[..]))
        );
        assert_eq!(overlay.read(&id(4), snapshot).unwrap(), None);
    }
    assert_eq!(snapshot(&id(1)), Some(&b"old"[..]));
}
