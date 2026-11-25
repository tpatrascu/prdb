use prdb_engine::audit::{AuditAction, AuditEvent};
use prdb_engine::commit::{
    CatalogWrite, CommitError, CommitRequest, ForeignKeyCheck, IndexWrite, RowWrite,
    commit_catalog_and_audit, commit_rows, commit_rows_and_indexes, commit_storage_changes,
};
use prdb_engine::index_locator::IndexLocator;
use prdb_engine::outcome::{InvocationOutcome, OutcomeStatus, lookup_outcome};
use prdb_engine::schema::{CATALOG, ROWS};
use prdb_engine::tuple::{ColumnSchema, ScalarType, ScalarValue, encode_tuple};
use redb::{Database, ReadableDatabase, ReadableTable};
use std::sync::{Arc, Barrier};

use super::helpers::*;

pub(super) fn membership_catalog_commits_reject_cycles_and_record_aborted_outcomes()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(40),
            id(9),
            9_999,
            &[
                principal_write(id(1), 2),
                principal_write(id(2), 2),
                principal_object_write(id(1), 2),
                principal_object_write(id(2), 2),
                membership_write(id(1), id(2)),
                dependency_write(id(1), id(2), 5),
            ],
            &[audit_event(40)],
        )?,
        1
    );
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(44),
            id(9),
            9_999,
            &[
                principal_write(id(4), 2),
                principal_object_write(id(4), 2),
                membership_write(id(1), id(4)),
            ],
            &[audit_event(44)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(42),
            id(9),
            9_999,
            &[
                principal_write(id(3), 1),
                principal_object_write(id(3), 1),
                membership_write(id(1), id(3)),
            ],
            &[audit_event(42)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(43),
            id(9),
            9_999,
            &[membership_write(id(4), id(2))],
            &[audit_event(43)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(41),
            id(9),
            9_999,
            &[membership_write(id(2), id(1))],
            &[audit_event(41)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    let read = database.begin_read()?;
    assert!(
        read.open_table(prdb_engine::schema::CATALOG)?
            .get(&membership_write(id(2), id(1)).key[..])?
            .is_none()
    );
    assert!(
        read.open_table(prdb_engine::schema::AUDIT)?
            .get(&[41; 16][..])?
            .is_none()
    );
    assert!(
        read.open_table(prdb_engine::schema::CATALOG)?
            .get(&principal_write(id(3), 1).key[..])?
            .is_none()
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&id(41)[..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&id(43)[..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&id(42)[..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn catalog_commit_requires_exact_membership_authority_edges()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let identities = [
        principal_write(id(21), 1),
        principal_write(id(22), 2),
        principal_object_write(id(21), 1),
        principal_object_write(id(22), 2),
    ];
    let membership = membership_write(id(21), id(22));
    let mut missing_edge = identities.to_vec();
    missing_edge.push(membership.clone());
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(70),
            id(9),
            9_999,
            &missing_edge,
            &[audit_event(70)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 0);

    let authority = dependency_write(id(21), id(22), 5);
    let mut complete = identities.to_vec();
    complete.extend([membership, authority]);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(71),
            id(9),
            9_999,
            &complete,
            &[audit_event(71)],
        )?,
        1
    );
    let extra = dependency_write(id(22), id(21), 5);
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(72),
            id(9),
            9_999,
            &[extra],
            &[audit_event(72)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    Ok(())
}

pub(super) fn database_scoped_authority_grants_use_the_bootstrap_database_id()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let user_id = id(73);
    let identities = [
        principal_write(user_id, 1),
        principal_object_write(user_id, 1),
    ];
    let database_id = id(0x33);
    let grant = grant_write(user_id, database_id, 12, 1, database_id, builtin_owner_id());
    let mut missing_edge = identities.to_vec();
    missing_edge.push(grant.clone());
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(74),
            id(9),
            9_999,
            &missing_edge,
            &[audit_event(74)],
        ),
        Err(CommitError::InvalidCatalog)
    );
    assert_eq!(commit_sequence(&database)?, 0);

    let mut complete = identities.to_vec();
    complete.push(grant);
    complete.push(dependency_write(user_id, database_id, 5));
    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(75),
            id(9),
            9_999,
            &complete,
            &[audit_event(75)],
        )?,
        1
    );
    Ok(())
}

pub(super) fn concurrent_membership_commits_cannot_publish_a_cycle()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    commit_catalog_and_audit(
        &database,
        id(50),
        id(9),
        9_999,
        &[
            principal_write(id(1), 2),
            principal_write(id(2), 2),
            principal_object_write(id(1), 2),
            principal_object_write(id(2), 2),
        ],
        &[audit_event(50)],
    )?;
    let barrier = Arc::new(Barrier::new(3));
    let (left, right) = std::thread::scope(|scope| {
        let left_barrier = barrier.clone();
        let left_database = &database;
        let left = scope.spawn(move || {
            left_barrier.wait();
            commit_catalog_and_audit(
                left_database,
                id(51),
                id(9),
                9_999,
                &[
                    membership_write(id(1), id(2)),
                    dependency_write(id(1), id(2), 5),
                    principal_epoch_update(id(1), 2, 2),
                ],
                &[audit_event(51)],
            )
        });
        let right_barrier = barrier.clone();
        let right_database = &database;
        let right = scope.spawn(move || {
            right_barrier.wait();
            commit_catalog_and_audit(
                right_database,
                id(52),
                id(9),
                9_999,
                &[
                    membership_write(id(2), id(1)),
                    dependency_write(id(2), id(1), 5),
                    principal_epoch_update(id(2), 2, 2),
                ],
                &[audit_event(52)],
            )
        });
        barrier.wait();
        (left.join().unwrap(), right.join().unwrap())
    });
    let outcomes = [left, right];
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        outcomes
            .iter()
            .filter(|result| **result == Err(CommitError::InvalidCatalog))
            .count(),
        1
    );
    assert_eq!(commit_sequence(&database)?, 2);
    let read = database.begin_read()?;
    let catalog = read.open_table(prdb_engine::schema::CATALOG)?;
    assert_eq!(
        [
            catalog
                .get(&membership_write(id(1), id(2)).key[..])?
                .is_some(),
            catalog
                .get(&membership_write(id(2), id(1)).key[..])?
                .is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count(),
        1
    );
    let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
    let statuses = [id(51), id(52)].map(|invocation_id| {
        InvocationOutcome::decode(outcomes.get(&invocation_id[..]).unwrap().unwrap().value())
            .unwrap()
            .status
    });
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == OutcomeStatus::Committed)
            .count(),
        1
    );
    assert_eq!(
        statuses
            .iter()
            .filter(|status| **status == OutcomeStatus::Aborted)
            .count(),
        1
    );
    Ok(())
}

pub(super) fn catalog_audit_metadata_and_outcome_commit_together()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let (catalog_key, catalog_payload) = dependency_record([1; 16], [2; 16], 1);
    let write = CatalogWrite {
        key: catalog_key.clone(),
        expected_value: None,
        payload: Some(catalog_payload.clone()),
    };
    let catalog_writes = [
        object_write_with_parent([1; 16], 4, Some([2; 16])),
        object_write([2; 16], 3),
        column_write([1; 16], [2; 16], "object_1"),
        table_write([2; 16], builtin_id(1)),
        write.clone(),
    ];

    assert_eq!(
        commit_catalog_and_audit(
            &database,
            id(1),
            id(9),
            9_999,
            &catalog_writes,
            &[audit_event(10)],
        )?,
        1
    );
    {
        let read = database.begin_read()?;
        let catalog = read.open_table(prdb_engine::schema::CATALOG)?;
        assert_eq!(
            catalog.get(&catalog_key[..])?.unwrap().value(),
            catalog_payload
        );
        let audit = read.open_table(prdb_engine::schema::AUDIT)?;
        let event_bytes = audit.get(&[10; 16][..])?.unwrap().value().to_vec();
        assert_eq!(AuditEvent::decode(&event_bytes)?, audit_event(10));
        let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
        assert_eq!(
            InvocationOutcome::decode(outcomes.get(&id(1)[..])?.unwrap().value())?.status,
            OutcomeStatus::Committed
        );
    }

    assert_eq!(
        commit_catalog_and_audit(&database, id(2), id(9), 9_999, &[write], &[audit_event(20)],),
        Err(CommitError::Conflict)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    let read = database.begin_read()?;
    assert!(
        read.open_table(prdb_engine::schema::AUDIT)?
            .get(&[20; 16][..])?
            .is_none()
    );
    assert_eq!(
        InvocationOutcome::decode(
            read.open_table(prdb_engine::schema::OUTCOMES)?
                .get(&id(2)[..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn one_redb_commit_can_publish_rows_indexes_catalog_audit_and_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_key = key(7);
    let index_key = index_key(&row_key);
    let (catalog_key, catalog_payload) = dependency_record([6; 16], [7; 16], 1);

    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(4),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: row_key,
                    expected_version: None,
                    payload: Some(b"row".to_vec()),
                }],
                indexes: &[IndexWrite {
                    key: index_key.clone(),
                    row_key,
                    expected_version: None,
                    present: true,
                    unique: false,
                    null_exempt: false,
                }],
                foreign_keys: &[],
                catalog_guards: &[],
                catalog: &[
                    object_write_with_parent([6; 16], 4, Some([7; 16])),
                    object_write([7; 16], 3),
                    column_write([6; 16], [7; 16], "object_6"),
                    table_write([7; 16], builtin_id(1)),
                    CatalogWrite {
                        key: catalog_key.clone(),
                        expected_value: None,
                        payload: Some(catalog_payload.clone()),
                    },
                ],
                audit: &[audit_event(30)],
            },
        )?,
        1
    );

    let read = database.begin_read()?;
    assert_eq!(row(&database, &row_key)?.unwrap().payload, b"row");
    let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
    let locator = indexes.get(&index_key[..])?.unwrap().value().to_vec();
    assert_eq!(
        IndexLocator::decode_for_key(&index_key, &locator)?.row_version,
        1
    );
    assert!(
        read.open_table(prdb_engine::schema::CATALOG)?
            .get(&catalog_key[..])?
            .is_some()
    );
    assert!(
        read.open_table(prdb_engine::schema::AUDIT)?
            .get(&[30; 16][..])?
            .is_some()
    );
    assert_eq!(commit_sequence(&database)?, 1);
    let conflict = commit_storage_changes(
        &database,
        CommitRequest {
            invocation_id: id(5),
            principal_id: id(9),
            retention_deadline_us: 9_999,
            rows: &[RowWrite {
                key: row_key,
                expected_version: Some(1),
                payload: Some(b"must-rollback".to_vec()),
            }],
            indexes: &[IndexWrite {
                key: index_key.clone(),
                row_key,
                expected_version: Some(1),
                present: true,
                unique: false,
                null_exempt: false,
            }],
            foreign_keys: &[],
            catalog_guards: &[],
            catalog: &[CatalogWrite {
                key: catalog_key.clone(),
                expected_value: None,
                payload: Some(catalog_payload),
            }],
            audit: &[audit_event(31)],
        },
    );
    assert_eq!(conflict, Err(CommitError::Conflict));
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(row(&database, &row_key)?.unwrap().payload, b"row");
    let verify = database.begin_read()?;
    let index_value = verify
        .open_table(prdb_engine::schema::INDEXES)?
        .get(&index_key[..])?
        .unwrap()
        .value()
        .to_vec();
    assert_eq!(
        IndexLocator::decode_for_key(&index_key, &index_value)?.row_version,
        1
    );
    assert!(
        verify
            .open_table(prdb_engine::schema::CATALOG)?
            .get(&catalog_key[..])?
            .is_some()
    );
    assert!(
        verify
            .open_table(prdb_engine::schema::AUDIT)?
            .get(&[31; 16][..])?
            .is_none()
    );
    assert_eq!(
        InvocationOutcome::decode(
            verify
                .open_table(prdb_engine::schema::OUTCOMES)?
                .get(&id(5)[..])?
                .unwrap()
                .value()
        )?
        .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn oversized_index_key_is_rejected_before_publication()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_key = key(7);
    let mut index_key = vec![0x44; 16];
    index_key.resize(prdb_engine::index_key::MAX_INDEX_KEY_BYTES + 1 - 16, 0x01);
    index_key.extend_from_slice(&row_key[16..]);

    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(6),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: None,
                payload: Some(b"row".to_vec()),
            }],
            &[IndexWrite {
                key: index_key,
                row_key,
                expected_version: None,
                present: true,
                unique: false,
                null_exempt: false,
            }],
        ),
        Err(CommitError::InvalidIndex)
    );
    assert_eq!(commit_sequence(&database)?, 0);
    assert!(row(&database, &row_key)?.is_none());

    let mut boundary_key = vec![0x44; 16];
    boundary_key.resize(prdb_engine::index_key::MAX_INDEX_KEY_BYTES - 16, 0x01);
    boundary_key.extend_from_slice(&row_key[16..]);
    assert_eq!(
        boundary_key.len(),
        prdb_engine::index_key::MAX_INDEX_KEY_BYTES
    );
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(7),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: None,
                payload: Some(b"row".to_vec()),
            }],
            &[IndexWrite {
                key: boundary_key,
                row_key,
                expected_version: None,
                present: true,
                unique: false,
                null_exempt: false,
            }],
        )?,
        1
    );
    assert!(row(&database, &row_key)?.is_some());
    Ok(())
}

pub(super) fn row_updates_refresh_retained_index_locator_versions()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let mut row_key = [0x10; 32];
    row_key[16..].fill(0x31);
    let index_key = index_key(&row_key);
    commit_rows_and_indexes(
        &database,
        id(40),
        id(9),
        9_999,
        &[RowWrite {
            key: row_key,
            expected_version: None,
            payload: Some(bytea_tuple(b"first")),
        }],
        &[IndexWrite {
            key: index_key.clone(),
            row_key,
            expected_version: None,
            present: true,
            unique: false,
            null_exempt: false,
        }],
    )?;

    assert_eq!(
        commit_rows(
            &database,
            id(41),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(1),
                payload: Some(bytea_tuple(b"second")),
            }],
        )?,
        2
    );
    let read = database.begin_read()?;
    let locator = read
        .open_table(prdb_engine::schema::INDEXES)?
        .get(&index_key[..])?
        .unwrap()
        .value()
        .to_vec();
    assert_eq!(
        IndexLocator::decode_for_key(&index_key, &locator)?.row_version,
        2
    );
    drop(read);

    assert_eq!(
        commit_rows(
            &database,
            id(42),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(2),
                payload: None,
            }],
        )?,
        3
    );
    let mut reverse_key = row_key.to_vec();
    reverse_key.extend_from_slice(&index_key[..index_key.len() - 16]);
    let read = database.begin_read()?;
    assert!(
        read.open_table(prdb_engine::schema::INDEXES)?
            .get(&index_key[..])?
            .is_none()
    );
    assert!(
        read.open_table(prdb_engine::schema::ROW_INDEXES)?
            .get(&reverse_key[..])?
            .is_none()
    );
    assert!(row(&database, &row_key)?.is_none());
    prdb_engine::startup::validate_persisted_records(&database)?;
    Ok(())
}

pub(super) fn row_and_index_locators_publish_and_remove_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let row_key = key(7);
    let index_key = index_key(&row_key);

    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(1),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: None,
                payload: Some(b"v1".to_vec())
            }],
            &[IndexWrite {
                key: index_key.clone(),
                row_key,
                expected_version: None,
                present: true,
                unique: false,
                null_exempt: false,
            }],
        )?,
        1
    );
    {
        let read = database.begin_read()?;
        let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
        let locator = indexes.get(&index_key[..])?.unwrap().value().to_vec();
        assert_eq!(
            IndexLocator::decode_for_key(&index_key, &locator)?.row_version,
            1
        );
    }

    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(2),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(1),
                payload: Some(b"v2".to_vec())
            }],
            &[IndexWrite {
                key: index_key.clone(),
                row_key,
                expected_version: Some(1),
                present: true,
                unique: false,
                null_exempt: false,
            }],
        )?,
        2
    );
    {
        let read = database.begin_read()?;
        let indexes = read.open_table(prdb_engine::schema::INDEXES)?;
        let locator = indexes.get(&index_key[..])?.unwrap().value().to_vec();
        assert_eq!(
            IndexLocator::decode_for_key(&index_key, &locator)?.row_version,
            2
        );
    }

    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(8),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(2),
                payload: Some(b"must-rollback".to_vec())
            }],
            &[IndexWrite {
                key: index_key.clone(),
                row_key,
                expected_version: Some(1),
                present: true,
                unique: false,
                null_exempt: false,
            }],
        ),
        Err(CommitError::Conflict)
    );
    assert_eq!(commit_sequence(&database)?, 2);
    assert_eq!(row(&database, &row_key)?.unwrap().payload, b"v2");

    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(3),
            id(9),
            9_999,
            &[RowWrite {
                key: row_key,
                expected_version: Some(2),
                payload: None
            }],
            &[IndexWrite {
                key: index_key.clone(),
                row_key,
                expected_version: Some(2),
                present: false,
                unique: false,
                null_exempt: false,
            }],
        )?,
        3
    );
    let read = database.begin_read()?;
    assert!(
        read.open_table(prdb_engine::schema::INDEXES)?
            .get(&index_key[..])?
            .is_none()
    );
    assert!(row(&database, &row_key)?.is_none());
    Ok(())
}

pub(super) fn serialized_unique_index_check_rejects_duplicates_and_exempts_null_tuples()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let first_row = key(31);
    let first_index = index_key(&first_row);
    commit_rows_and_indexes(
        &database,
        id(31),
        id(9),
        9_999,
        &[RowWrite {
            key: first_row,
            expected_version: None,
            payload: Some(b"first".to_vec()),
        }],
        &[IndexWrite {
            key: first_index,
            row_key: first_row,
            expected_version: None,
            present: true,
            unique: true,
            null_exempt: false,
        }],
    )?;

    let second_row = key(32);
    let second_index = index_key(&second_row);
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(32),
            id(9),
            9_999,
            &[RowWrite {
                key: second_row,
                expected_version: None,
                payload: Some(b"duplicate".to_vec()),
            }],
            &[IndexWrite {
                key: second_index.clone(),
                row_key: second_row,
                expected_version: None,
                present: true,
                unique: true,
                null_exempt: false,
            }],
        ),
        Err(CommitError::UniqueViolation)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert!(row(&database, &second_row)?.is_none());

    let null_index = prdb_engine::index_key::compose_key(
        [0x44; 16],
        &[0xff],
        second_row[16..].try_into().unwrap(),
    );
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(33),
            id(9),
            9_999,
            &[RowWrite {
                key: second_row,
                expected_version: None,
                payload: Some(b"null-key".to_vec()),
            }],
            &[IndexWrite {
                key: null_index,
                row_key: second_row,
                expected_version: None,
                present: true,
                unique: true,
                null_exempt: true,
            }],
        )?,
        2
    );
    assert_eq!(row(&database, &second_row)?.unwrap().payload, b"null-key");

    let third_row = key(33);
    let third_null_index = prdb_engine::index_key::compose_key(
        [0x44; 16],
        &[0xff],
        third_row[16..].try_into().unwrap(),
    );
    assert_eq!(
        commit_rows_and_indexes(
            &database,
            id(34),
            id(9),
            9_999,
            &[RowWrite {
                key: third_row,
                expected_version: None,
                payload: Some(b"another-null-key".to_vec()),
            }],
            &[IndexWrite {
                key: third_null_index,
                row_key: third_row,
                expected_version: None,
                present: true,
                unique: true,
                null_exempt: true,
            }],
        )?,
        3
    );
    assert_eq!(
        row(&database, &third_row)?.unwrap().payload,
        b"another-null-key"
    );
    Ok(())
}

pub(super) fn foreign_key_commit_checks_child_and_parent_changes_against_final_rows()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let source_table_id = [0x55; 16];
    let target_table_id = [0x66; 16];
    let source_key = table_row_key(source_table_id, [1; 16]);
    let null_source_key = table_row_key(source_table_id, [3; 16]);
    let target_key = table_row_key(target_table_id, [2; 16]);
    let column = ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    };
    let foreign_key = ForeignKeyCheck {
        source_table_id,
        source_schema_version: 1,
        source_schema: vec![column.clone()],
        source_ordinals: vec![0],
        target_table_id,
        target_schema_version: 1,
        target_schema: vec![column.clone()],
        target_ordinals: vec![0],
    };
    let parent = encode_tuple(
        std::slice::from_ref(&column),
        1,
        &[Some(ScalarValue::Bytea(b"parent".to_vec()))],
    )?;
    commit_rows(
        &database,
        id(101),
        id(9),
        9_999,
        &[RowWrite {
            key: target_key,
            expected_version: None,
            payload: Some(parent.clone()),
        }],
    )?;

    let missing_parent = encode_tuple(
        std::slice::from_ref(&column),
        1,
        &[Some(ScalarValue::Bytea(b"missing".to_vec()))],
    )?;
    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(102),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: source_key,
                    expected_version: None,
                    payload: Some(missing_parent),
                }],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        ),
        Err(CommitError::ForeignKeyViolation)
    );
    assert!(row(&database, &source_key)?.is_none());
    assert_eq!(commit_sequence(&database)?, 1);

    let null_child = encode_tuple(std::slice::from_ref(&column), 1, &[None])?;
    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(109),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: null_source_key,
                    expected_version: None,
                    payload: Some(null_child),
                }],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        )?,
        2
    );

    let child = encode_tuple(
        std::slice::from_ref(&column),
        1,
        &[Some(ScalarValue::Bytea(b"parent".to_vec()))],
    )?;
    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(103),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: source_key,
                    expected_version: None,
                    payload: Some(child),
                }],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        )?,
        3
    );

    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(104),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: target_key,
                    expected_version: Some(1),
                    payload: None,
                }],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        ),
        Err(CommitError::ForeignKeyViolation)
    );
    assert!(row(&database, &target_key)?.is_some());

    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(105),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[
                    RowWrite {
                        key: source_key,
                        expected_version: Some(3),
                        payload: None,
                    },
                    RowWrite {
                        key: null_source_key,
                        expected_version: Some(2),
                        payload: None,
                    },
                    RowWrite {
                        key: target_key,
                        expected_version: Some(1),
                        payload: None,
                    },
                ],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        )?,
        4
    );
    assert!(row(&database, &source_key)?.is_none());
    assert!(row(&database, &null_source_key)?.is_none());
    assert!(row(&database, &target_key)?.is_none());
    Ok(())
}

pub(super) fn foreign_key_row_visit_limit_aborts_the_whole_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let source_table_id = [0x55; 16];
    let target_table_id = [0x66; 16];
    let column = ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    };
    let parent = bytea_tuple(b"parent");
    let mut initial_rows = Vec::with_capacity(256);
    let mut updated_child_key = [0; 32];
    for row_number in 0..128u8 {
        let parent_key = table_row_key(target_table_id, [row_number; 16]);
        let child_key = table_row_key(source_table_id, [row_number; 16]);
        if row_number == 0 {
            updated_child_key = child_key;
        }
        initial_rows.push(RowWrite {
            key: parent_key,
            expected_version: None,
            payload: Some(parent.clone()),
        });
        initial_rows.push(RowWrite {
            key: child_key,
            expected_version: None,
            payload: Some(parent.clone()),
        });
    }
    commit_rows(&database, id(110), id(9), 9_999, &initial_rows)?;

    let foreign_key = ForeignKeyCheck {
        source_table_id,
        source_schema_version: 1,
        source_schema: vec![column.clone()],
        source_ordinals: vec![0],
        target_table_id,
        target_schema_version: 1,
        target_schema: vec![column],
        target_ordinals: vec![0],
    };
    let foreign_keys = vec![foreign_key; 256];
    let result = commit_storage_changes(
        &database,
        CommitRequest {
            invocation_id: id(111),
            principal_id: id(9),
            retention_deadline_us: 9_999,
            rows: &[RowWrite {
                key: updated_child_key,
                expected_version: Some(1),
                payload: Some(parent),
            }],
            indexes: &[],
            foreign_keys: &foreign_keys,
            catalog_guards: &[],
            catalog: &[],
            audit: &[],
        },
    );
    assert_eq!(result, Err(CommitError::ForeignKeyValidationLimitExceeded));
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(row(&database, &updated_child_key)?.unwrap().row_version, 1);
    let read = database.begin_read()?;
    let encoded_outcome = read
        .open_table(prdb_engine::schema::OUTCOMES)?
        .get(&id(111)[..])?
        .unwrap()
        .value()
        .to_vec();
    assert_eq!(
        InvocationOutcome::decode(&encoded_outcome)?.status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn foreign_key_comparison_key_budget_aborts_large_keys_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let source_table_id = [0x55; 16];
    let target_table_id = [0x66; 16];
    let column = ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: false,
    };
    let mut initial_rows = Vec::with_capacity(256);
    let mut updated_child_key = [0; 32];
    let mut updated_child_payload = Vec::new();
    for row_number in 0..128u8 {
        let mut value = vec![row_number.wrapping_add(1); 70 * 1024];
        value[..8].copy_from_slice(&(row_number as u64).to_be_bytes());
        let payload = encode_tuple(
            std::slice::from_ref(&column),
            1,
            &[Some(ScalarValue::Bytea(value))],
        )?;
        let parent_key = table_row_key(target_table_id, [row_number; 16]);
        let child_key = table_row_key(source_table_id, [row_number; 16]);
        if row_number == 0 {
            updated_child_key = child_key;
            updated_child_payload = payload.clone();
        }
        initial_rows.push(RowWrite {
            key: parent_key,
            expected_version: None,
            payload: Some(payload.clone()),
        });
        initial_rows.push(RowWrite {
            key: child_key,
            expected_version: None,
            payload: Some(payload),
        });
    }
    commit_rows(&database, id(112), id(9), 9_999, &initial_rows)?;

    let foreign_key = ForeignKeyCheck {
        source_table_id,
        source_schema_version: 1,
        source_schema: vec![column.clone()],
        source_ordinals: vec![0],
        target_table_id,
        target_schema_version: 1,
        target_schema: vec![column],
        target_ordinals: vec![0],
    };
    assert_eq!(
        commit_storage_changes(
            &database,
            CommitRequest {
                invocation_id: id(113),
                principal_id: id(9),
                retention_deadline_us: 9_999,
                rows: &[RowWrite {
                    key: updated_child_key,
                    expected_version: Some(1),
                    payload: Some(updated_child_payload),
                }],
                indexes: &[],
                foreign_keys: std::slice::from_ref(&foreign_key),
                catalog_guards: &[],
                catalog: &[],
                audit: &[],
            },
        ),
        Err(CommitError::ForeignKeyValidationLimitExceeded)
    );
    assert_eq!(commit_sequence(&database)?, 1);
    assert_eq!(row(&database, &updated_child_key)?.unwrap().row_version, 1);
    Ok(())
}

pub(super) fn dependency_graph_scan_limit_aborts_catalog_publication_atomically()
-> Result<(), Box<dyn std::error::Error>> {
    const MAX_FAMILY_RECORDS: usize = 65_536;
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let prefix = 15u16.to_be_bytes();
    let write = database.begin_write()?;
    let mut catalog = write.open_table(CATALOG)?;
    let mut existing_count = 0;
    for entry in catalog.range::<&[u8]>(prefix.as_slice()..)? {
        let (key, _) = entry?;
        if !key.value().starts_with(&prefix) {
            break;
        }
        existing_count += 1;
    }
    for sequence in 1..=(MAX_FAMILY_RECORDS - existing_count) as u64 {
        let mut dependent_id = [0xa5; 16];
        dependent_id[..8].copy_from_slice(&sequence.to_be_bytes());
        let write = dependency_write(dependent_id, [0xa6; 16], 5);
        catalog.insert(
            write.key.as_slice(),
            write.payload.as_ref().unwrap().as_slice(),
        )?;
    }
    drop(catalog);
    write.commit()?;

    let overflow = dependency_write([0xf5; 16], [0xf6; 16], 5);
    let invocation_id = id(0xf7);
    let actor = builtin_owner_id();
    let mut event = audit_event(0xf8);
    event.actor_id = Some(actor);
    event.correlation_id = invocation_id;
    event.action = AuditAction::Ddl;
    assert!(matches!(
        commit_catalog_and_audit(
            &database,
            invocation_id,
            actor,
            9_999,
            std::slice::from_ref(&overflow),
            &[event],
        ),
        Err(CommitError::InvalidCatalog)
    ));
    assert_eq!(commit_sequence(&database)?, 0);
    assert!(
        database
            .begin_read()?
            .open_table(CATALOG)?
            .get(overflow.key.as_slice())?
            .is_none()
    );
    assert_eq!(
        lookup_outcome(&database, invocation_id, actor, 1)?
            .ok_or("missing bounded dependency validation outcome")?
            .status,
        OutcomeStatus::Aborted
    );
    Ok(())
}

pub(super) fn concurrent_foreign_key_insert_and_parent_delete_cannot_both_commit()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;
    let source_table_id = [0x55; 16];
    let target_table_id = [0x66; 16];
    let source_key = table_row_key(source_table_id, [1; 16]);
    let target_key = table_row_key(target_table_id, [2; 16]);
    let column = ColumnSchema {
        scalar_type: ScalarType::Bytea,
        nullable: true,
    };
    let foreign_key = ForeignKeyCheck {
        source_table_id,
        source_schema_version: 1,
        source_schema: vec![column.clone()],
        source_ordinals: vec![0],
        target_table_id,
        target_schema_version: 1,
        target_schema: vec![column.clone()],
        target_ordinals: vec![0],
    };
    let value = encode_tuple(
        std::slice::from_ref(&column),
        1,
        &[Some(ScalarValue::Bytea(b"parent".to_vec()))],
    )?;
    commit_rows(
        &database,
        id(106),
        id(9),
        9_999,
        &[RowWrite {
            key: target_key,
            expected_version: None,
            payload: Some(value.clone()),
        }],
    )?;
    let source_write = RowWrite {
        key: source_key,
        expected_version: None,
        payload: Some(value),
    };
    let target_write = RowWrite {
        key: target_key,
        expected_version: Some(1),
        payload: None,
    };
    let barrier = Arc::new(Barrier::new(3));
    let (child_result, parent_result) = std::thread::scope(|scope| {
        let child_barrier = barrier.clone();
        let child_fk = foreign_key.clone();
        let child_database = &database;
        let child = scope.spawn(move || {
            child_barrier.wait();
            commit_storage_changes(
                child_database,
                CommitRequest {
                    invocation_id: id(107),
                    principal_id: id(9),
                    retention_deadline_us: 9_999,
                    rows: std::slice::from_ref(&source_write),
                    indexes: &[],
                    foreign_keys: std::slice::from_ref(&child_fk),
                    catalog_guards: &[],
                    catalog: &[],
                    audit: &[],
                },
            )
        });
        let parent_barrier = barrier.clone();
        let parent_database = &database;
        let parent = scope.spawn(move || {
            parent_barrier.wait();
            commit_storage_changes(
                parent_database,
                CommitRequest {
                    invocation_id: id(108),
                    principal_id: id(9),
                    retention_deadline_us: 9_999,
                    rows: std::slice::from_ref(&target_write),
                    indexes: &[],
                    foreign_keys: std::slice::from_ref(&foreign_key),
                    catalog_guards: &[],
                    catalog: &[],
                    audit: &[],
                },
            )
        });
        barrier.wait();
        (child.join().unwrap(), parent.join().unwrap())
    });
    assert_ne!(child_result.is_ok(), parent_result.is_ok());
    assert!(matches!(
        child_result.as_ref().err().or(parent_result.as_ref().err()),
        Some(CommitError::ForeignKeyViolation)
    ));
    assert_eq!(commit_sequence(&database)?, 2);
    let read = database.begin_read()?;
    let child_exists = read.open_table(ROWS)?.get(&source_key[..])?.is_some();
    let parent_exists = read.open_table(ROWS)?.get(&target_key[..])?.is_some();
    assert_eq!(child_exists, parent_exists);
    Ok(())
}

pub(super) fn multirow_commit_assigns_one_sequence_and_conflict_aborts_the_whole_set()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = Fixture::new();
    let database = initialize(&fixture.path())?;

    assert_eq!(
        commit_rows(
            &database,
            id(1),
            id(9),
            9_999,
            &[
                RowWrite {
                    key: key(1),
                    expected_version: None,
                    payload: Some(b"a1".to_vec()),
                },
                RowWrite {
                    key: key(2),
                    expected_version: None,
                    payload: Some(b"b1".to_vec()),
                },
            ],
        )?,
        1
    );
    assert_eq!(commit_sequence(&database)?, 1);

    assert_eq!(
        commit_rows(
            &database,
            id(2),
            id(9),
            9_999,
            &[
                RowWrite {
                    key: key(1),
                    expected_version: Some(1),
                    payload: Some(b"a2".to_vec()),
                },
                RowWrite {
                    key: key(2),
                    expected_version: Some(1),
                    payload: Some(b"b2".to_vec()),
                },
            ],
        )?,
        2
    );
    assert_eq!(row(&database, &key(1))?.unwrap().row_version, 2);
    assert_eq!(row(&database, &key(2))?.unwrap().row_version, 2);

    let conflict = commit_rows(
        &database,
        id(3),
        id(9),
        9_999,
        &[
            RowWrite {
                key: key(1),
                expected_version: Some(1),
                payload: Some(b"stale".to_vec()),
            },
            RowWrite {
                key: key(2),
                expected_version: Some(2),
                payload: Some(b"must-rollback".to_vec()),
            },
        ],
    );
    assert_eq!(conflict, Err(CommitError::Conflict));
    assert_eq!(commit_sequence(&database)?, 2);
    assert_eq!(row(&database, &key(1))?.unwrap().payload, b"a2");
    assert_eq!(row(&database, &key(2))?.unwrap().payload, b"b2");
    {
        let read = database.begin_read()?;
        let outcomes = read.open_table(prdb_engine::schema::OUTCOMES)?;
        let outcome = outcomes.get(&id(2)[..])?.unwrap().value().to_vec();
        assert_eq!(
            InvocationOutcome::decode(&outcome)?,
            InvocationOutcome {
                status: OutcomeStatus::Committed,
                invocation_id: id(2),
                principal_id: id(9),
                commit_sequence: 2,
                retention_deadline_us: 9_999,
            }
        );
        let aborted = outcomes.get(&id(3)[..])?.unwrap().value().to_vec();
        assert_eq!(
            InvocationOutcome::decode(&aborted)?,
            InvocationOutcome {
                status: OutcomeStatus::Aborted,
                invocation_id: id(3),
                principal_id: id(9),
                commit_sequence: 0,
                retention_deadline_us: 9_999,
            }
        );
    }
    drop(database);
    let reopened = Database::open(fixture.path())?;
    assert_eq!(
        lookup_outcome(&reopened, id(2), id(9), 9_998)?,
        Some(InvocationOutcome {
            status: OutcomeStatus::Committed,
            invocation_id: id(2),
            principal_id: id(9),
            commit_sequence: 2,
            retention_deadline_us: 9_999,
        })
    );
    assert_eq!(lookup_outcome(&reopened, id(2), id(10), 9_998)?, None);
    assert_eq!(lookup_outcome(&reopened, id(2), id(9), 9_999)?, None);
    assert_eq!(
        lookup_outcome(&reopened, id(3), id(9), 9_998)?,
        Some(InvocationOutcome {
            status: OutcomeStatus::Aborted,
            invocation_id: id(3),
            principal_id: id(9),
            commit_sequence: 0,
            retention_deadline_us: 9_999,
        })
    );
    Ok(())
}
