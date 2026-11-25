use super::admission::*;
use super::catalog_helpers::*;
use super::*;
use crate::catalog::decode_registered_envelope;
use crate::metadata::{EngineMetadata, Lifecycle};
use crate::schema::{CATALOG, STATS};
use crate::snapshot::RowSnapshot;
use crate::statistics::{
    COMMITTED_POPULATION_KEY_PREFIX, CommittedPopulationRecord,
    MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS, StatisticsCollectionOutcome,
    StatisticsMaintenanceError, StatisticsRecordError,
};
use redb::ReadableDatabase;
use std::sync::atomic::{AtomicBool, Ordering};

pub(super) struct StatisticsMaintenancePermit<'a>(&'a AtomicBool);

impl Drop for StatisticsMaintenancePermit<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl<'db> StorageManager<'db> {
    /// Select the next table catalog key for the background statistics cursor.
    /// This reads only one table definition per tick and wraps at the end, so
    /// a large catalog does not require materializing its full table list.
    pub(crate) fn next_committed_population_table_id(
        &self,
        after: Option<[u8; 16]>,
    ) -> Result<Option<[u8; 16]>, StatisticsMaintenanceError> {
        if self.admissions.database_uncertain.load(Ordering::Acquire) {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        let catalog = read
            .open_table(CATALOG)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        let family_prefix = 5u16.to_be_bytes();
        let after_key = after.map(|table_id| id_catalog_key(5, table_id));

        for pass in 0..if after_key.is_some() { 2 } else { 1 } {
            let start = if pass == 0 {
                after_key.as_deref().unwrap_or(family_prefix.as_slice())
            } else {
                family_prefix.as_slice()
            };
            for entry in catalog
                .range(start..)
                .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?
            {
                let (key, value) = entry
                    .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
                let key = key.value().to_vec();
                if !key.starts_with(&family_prefix) {
                    break;
                }
                if pass == 0 && after_key.as_ref().is_some_and(|cursor| key <= *cursor) {
                    continue;
                }
                if key.len() != 19 || key[2] != 1 {
                    return Err(StatisticsMaintenanceError::Storage(
                        "invalid table catalog key".into(),
                    ));
                }
                let bytes = value.value().to_vec();
                let record = decode_registered_envelope(&bytes)
                    .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
                if record.family_code != 5 || record.logical_key != key {
                    return Err(StatisticsMaintenanceError::Storage(
                        "invalid table catalog record".into(),
                    ));
                }
                return Ok(Some(key[3..19].try_into().map_err(|_| {
                    StatisticsMaintenanceError::Storage("invalid table catalog ID".into())
                })?));
            }
        }
        Ok(None)
    }

    /// Read a committed-population record only when it matches engine metadata
    /// from the same fresh redb snapshot. The count stays within trusted
    /// storage and is never included in the maintenance IPC result.
    pub(crate) fn current_committed_population_record(
        &self,
        table_id: [u8; 16],
    ) -> Result<Option<CommittedPopulationRecord>, StatisticsMaintenanceError> {
        if table_id == [0; 16] || self.admissions.database_uncertain.load(Ordering::Acquire) {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }
        let read = self
            .database
            .begin_read()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        let snapshot = RowSnapshot::new(read);
        let metadata_bytes = snapshot
            .get_engine_metadata_value()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?
            .ok_or_else(|| StatisticsMaintenanceError::Storage("missing engine metadata".into()))?;
        let metadata = EngineMetadata::decode(&metadata_bytes)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        if metadata.lifecycle != Lifecycle::Ready {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }

        let mut key = Vec::with_capacity(COMMITTED_POPULATION_KEY_PREFIX.len() + table_id.len());
        key.extend_from_slice(COMMITTED_POPULATION_KEY_PREFIX);
        key.extend_from_slice(&table_id);
        let stats = snapshot
            .read_transaction()
            .open_table(STATS)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        let Some(value) = stats
            .get(key.as_slice())
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let record = CommittedPopulationRecord::decode_record(table_id, value.value())
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        Ok((record.snapshot_sequence() == metadata.commit_sequence).then_some(record))
    }

    /// Collect a bounded committed row population from one redb snapshot.
    /// Exact global counts are stored only in the protected stats table; this
    /// public maintenance outcome omits the count. A trusted internal consumer
    /// must validate the stored source commit sequence before reuse. The method
    /// is not exposed over worker or planner IPC.
    pub fn collect_committed_table_population(
        &self,
        table_id: [u8; 16],
    ) -> Result<StatisticsCollectionOutcome, StatisticsMaintenanceError> {
        if table_id == [0; 16] || self.admissions.database_uncertain.load(Ordering::Acquire) {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }
        self.admissions
            .statistics_collection_active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| StatisticsMaintenanceError::Busy)?;
        let _permit = StatisticsMaintenancePermit(&self.admissions.statistics_collection_active);

        let maximum = self
            .admissions
            .statistics_collection_limit
            .load(Ordering::Acquire)
            .min(MAX_CONFIGURABLE_COMMITTED_POPULATION_ROWS as usize) as u64;
        if maximum == 0 {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }

        let read = self
            .database
            .begin_read()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        let snapshot = RowSnapshot::new(read);
        let metadata_bytes = snapshot
            .get_engine_metadata_value()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?
            .ok_or_else(|| StatisticsMaintenanceError::Storage("missing engine metadata".into()))?;
        let metadata = EngineMetadata::decode(&metadata_bytes)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        if metadata.lifecycle != Lifecycle::Ready {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }
        let mut catalog_key = Vec::with_capacity(19);
        catalog_key.extend_from_slice(&5u16.to_be_bytes());
        catalog_key.push(1);
        catalog_key.extend_from_slice(&table_id);
        if snapshot
            .get_catalog_record(&catalog_key)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?
            .is_none()
        {
            return Err(StatisticsMaintenanceError::InvalidRequest);
        }

        let mut row_count = 0u64;
        let mut after_row_id = None;
        let result = loop {
            let remaining = maximum.saturating_add(1).saturating_sub(row_count);
            if remaining == 0 {
                break CommittedPopulationRecord::Unknown {
                    table_id,
                    snapshot_sequence: metadata.commit_sequence,
                };
            }
            let page_limit = remaining.min(crate::snapshot::MAX_ROW_SCAN_PAGE as u64) as usize;
            let (row_ids, complete) = snapshot
                .scan_row_ids_after(table_id, after_row_id, page_limit)
                .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
            row_count = row_count.saturating_add(row_ids.len() as u64);
            if row_count > maximum {
                break CommittedPopulationRecord::Unknown {
                    table_id,
                    snapshot_sequence: metadata.commit_sequence,
                };
            }
            if complete {
                break CommittedPopulationRecord::Exact {
                    table_id,
                    snapshot_sequence: metadata.commit_sequence,
                    rows: row_count,
                };
            }
            after_row_id = row_ids.last().copied();
        };
        drop(snapshot);

        let record = result.encode_record();
        CommittedPopulationRecord::decode_record(result.table_id(), &record).map_err(|error| {
            let reason = match error {
                StatisticsRecordError::InvalidLength => "invalid record length",
                StatisticsRecordError::InvalidMagic => "invalid record magic",
                StatisticsRecordError::InvalidStatus => "invalid record status",
                StatisticsRecordError::InvalidValue => "invalid record value",
            };
            StatisticsMaintenanceError::Storage(reason.to_owned())
        })?;
        let mut key = Vec::with_capacity(COMMITTED_POPULATION_KEY_PREFIX.len() + table_id.len());
        key.extend_from_slice(COMMITTED_POPULATION_KEY_PREFIX);
        key.extend_from_slice(&table_id);
        let mut write = self
            .database
            .begin_write()
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        write
            .set_durability(redb::Durability::Immediate)
            .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        write.set_two_phase_commit(true);
        {
            let mut stats = write
                .open_table(STATS)
                .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
            stats
                .insert(key.as_slice(), record.as_slice())
                .map_err(|error| StatisticsMaintenanceError::Storage(error.to_string()))?;
        }
        if let Err(error) = write.commit() {
            mark_database_uncertain(self.database);
            return Err(StatisticsMaintenanceError::Storage(error.to_string()));
        }
        Ok(match result {
            CommittedPopulationRecord::Exact { .. } => StatisticsCollectionOutcome::Exact,
            CommittedPopulationRecord::Unknown { .. } => StatisticsCollectionOutcome::Unknown,
        })
    }

    /// Prune one bounded batch of expired durable outcomes. Storage failures
    /// quarantine this database handle; the owning service must reopen it.
    pub(crate) fn prune_expired_outcomes(
        &self,
        now_us: i64,
        max_scanned: usize,
    ) -> Result<crate::outcome::OutcomePruneResult, crate::outcome::OutcomeMaintenanceError> {
        let result = crate::outcome::prune_expired_outcomes(self.database, now_us, max_scanned);
        self.admissions
            .live
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .record_outcome_maintenance(&result);
        result
    }
}
