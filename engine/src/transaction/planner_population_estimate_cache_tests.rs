use crate::transaction::estimates::cached_exact_estimate;
use crate::transaction::planner_session::{PopulationEstimateCacheKey, SummaryEstimateCacheKey};
use std::cell::Cell;
use std::collections::HashMap;

fn key(maximum_rows: u64) -> PopulationEstimateCacheKey {
    PopulationEstimateCacheKey {
        relation_id: [1; 16],
        schema_id: [2; 16],
        schema_version: 3,
        maximum_rows,
    }
}

#[test]
fn exact_population_cache_hits_and_private_write_generation_invalidates() {
    let mut cache = HashMap::new();
    let compute_calls = Cell::new(0);
    assert_eq!(
        cached_exact_estimate(&mut cache, key(128), 0, || {
            compute_calls.set(compute_calls.get() + 1);
            Ok::<_, ()>(Some(5))
        }),
        Ok(Some(5))
    );
    assert_eq!(
        cached_exact_estimate(&mut cache, key(128), 0, || {
            compute_calls.set(compute_calls.get() + 1);
            Ok::<_, ()>(Some(99))
        }),
        Ok(Some(5))
    );
    assert_eq!(compute_calls.get(), 1, "the exact result must be reused");

    assert_eq!(
        cached_exact_estimate(&mut cache, key(128), 1, || {
            compute_calls.set(compute_calls.get() + 1);
            Ok::<_, ()>(Some(6))
        }),
        Ok(Some(6))
    );
    assert_eq!(compute_calls.get(), 2);
    assert_eq!(cache.len(), 1, "old-generation entries are discarded");
}

#[test]
fn unknown_population_estimates_are_not_cached() {
    let mut cache = HashMap::<PopulationEstimateCacheKey, (u64, u64)>::new();
    let compute_calls = Cell::new(0);
    for _ in 0..2 {
        assert_eq!(
            cached_exact_estimate(&mut cache, key(128), 0, || {
                compute_calls.set(compute_calls.get() + 1);
                Ok::<_, ()>(None)
            }),
            Ok(None)
        );
    }
    assert_eq!(compute_calls.get(), 2);
    assert!(cache.is_empty());
}

#[test]
fn exact_column_summary_cache_preserves_order_and_generation() {
    let key = SummaryEstimateCacheKey {
        relation_id: [1; 16],
        schema_id: [2; 16],
        schema_version: 3,
        column_ids: vec![[4; 16], [5; 16]],
    };
    let reversed_key = SummaryEstimateCacheKey {
        column_ids: vec![[5; 16], [4; 16]],
        ..key.clone()
    };
    assert_ne!(
        key, reversed_key,
        "requested column order is part of the key"
    );

    let first = crate::statistics::AuthorizedScanSummary {
        visible_rows: 5,
        columns: vec![
            crate::statistics::ColumnPopulation {
                readable_values: 5,
                nulls: 0,
                redacted: 0,
            },
            crate::statistics::ColumnPopulation {
                readable_values: 4,
                nulls: 1,
                redacted: 0,
            },
        ],
    };
    let updated = crate::statistics::AuthorizedScanSummary {
        visible_rows: 6,
        columns: first.columns.clone(),
    };
    let compute_calls = Cell::new(0);
    let mut cache = HashMap::new();
    assert_eq!(
        cached_exact_estimate(&mut cache, key.clone(), 0, || {
            compute_calls.set(compute_calls.get() + 1);
            Ok::<_, ()>(Some(first.clone()))
        }),
        Ok(Some(first.clone()))
    );
    assert_eq!(
        cached_exact_estimate(&mut cache, key.clone(), 0, || {
            compute_calls.set(compute_calls.get() + 1);
            Ok::<_, ()>(Some(updated.clone()))
        }),
        Ok(Some(first))
    );
    assert_eq!(compute_calls.get(), 1);
    assert_eq!(
        cached_exact_estimate(&mut cache, key, 1, || {
            compute_calls.set(compute_calls.get() + 1);
            Ok::<_, ()>(Some(updated.clone()))
        }),
        Ok(Some(updated))
    );
    assert_eq!(compute_calls.get(), 2);
}
