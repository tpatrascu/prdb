use super::{AccessTarget, Grant, InvocationTransaction, Operation};
use crate::authorization::Subject;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn invocation_scope_intersects_requested_capabilities_with_catalog_grants() {
    static NEXT_FIXTURE: AtomicUsize = AtomicUsize::new(0);
    let root = std::env::temp_dir().join(format!(
        "prdb-requested-scope-{}-{}",
        std::process::id(),
        NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&root);
    let mut bootstrap = crate::bootstrap::initialize(&root, "scope_admin", i64::MAX)
        .expect("initialize scope fixture");
    bootstrap.take_secret().expect("take bootstrap secret");
    let instance = bootstrap.acknowledge().expect("publish scope fixture");
    let mut invocation =
        InvocationTransaction::begin(instance.database(), [7; 16], [8; 16], i64::MAX, 1, 64)
            .expect("begin invocation");
    let selected_table = AccessTarget::Table([1; 16]);
    let other_table = AccessTarget::Table([2; 16]);
    let grants = [
        Grant {
            subject: Subject::Principal([8; 16]),
            operation: Operation::Select,
            target: selected_table,
        },
        Grant {
            subject: Subject::Principal([8; 16]),
            operation: Operation::Delete,
            target: other_table,
        },
    ];
    invocation
        .restrict_to_requested_scope([(Operation::Select, selected_table)].into_iter().collect());
    assert_eq!(
        invocation.scope_from_grants(&grants),
        [(Operation::Select, selected_table)].into_iter().collect()
    );
    drop(invocation);
    drop(instance);
    std::fs::remove_dir_all(root).unwrap();
}
