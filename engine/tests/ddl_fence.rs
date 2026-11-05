use prdb_engine::ddl_fence::{DdlFenceError, DdlFenceManager};
use std::time::Duration;

fn id(value: u8) -> [u8; 16] {
    [value; 16]
}

#[test]
fn exclusive_fences_wait_for_writers_and_block_new_writers() {
    let manager = DdlFenceManager::new();
    let writer = manager.acquire_shared(&[id(1)], Duration::ZERO).unwrap();
    assert!(matches!(
        manager.acquire_exclusive(&[id(1)], Duration::from_millis(10)),
        Err(DdlFenceError::TimedOut)
    ));

    drop(writer);
    let ddl = manager.acquire_exclusive(&[id(1)], Duration::ZERO).unwrap();
    assert!(matches!(
        manager.acquire_shared(&[id(1)], Duration::from_millis(10)),
        Err(DdlFenceError::TimedOut)
    ));

    drop(ddl);
    drop(manager.acquire_shared(&[id(1)], Duration::ZERO).unwrap());
}

#[test]
fn multi_table_exclusive_fence_is_atomic_after_timeout() {
    let manager = DdlFenceManager::new();
    let reader = manager.acquire_shared(&[id(2)], Duration::ZERO).unwrap();
    assert!(matches!(
        manager.acquire_exclusive(&[id(1), id(2)], Duration::from_millis(10)),
        Err(DdlFenceError::TimedOut)
    ));

    let unrelated_writer = manager.acquire_shared(&[id(1)], Duration::ZERO).unwrap();
    drop(unrelated_writer);
    drop(reader);
    drop(
        manager
            .acquire_exclusive(&[id(2), id(1), id(2)], Duration::ZERO)
            .unwrap(),
    );
}

#[test]
fn shared_fences_coexist_and_reject_zero_table_ids() {
    let manager = DdlFenceManager::new();
    let first = manager.acquire_shared(&[id(3)], Duration::ZERO).unwrap();
    let second = manager.acquire_shared(&[id(3)], Duration::ZERO).unwrap();
    assert!(matches!(
        manager.acquire_exclusive(&[[0; 16]], Duration::ZERO),
        Err(DdlFenceError::InvalidTableId)
    ));
    drop((first, second));
    drop(manager.acquire_exclusive(&[], Duration::ZERO).unwrap());
}
