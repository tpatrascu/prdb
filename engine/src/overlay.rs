//! Bounded invocation-local staged row values. This module does not authorize writes.
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RowId {
    pub table: [u8; 16],
    pub row: [u8; 16],
}
#[derive(Debug, PartialEq, Eq)]
pub enum OverlayError {
    LimitExceeded,
    Aborted,
}
#[derive(Debug, PartialEq, Eq)]
pub enum StagedRow<'a> {
    Value(&'a [u8]),
    Delete,
}
#[derive(Debug, PartialEq, Eq)]
pub enum ResolvedRow<'overlay, 'snapshot> {
    OverlayValue(&'overlay [u8]),
    SnapshotValue(&'snapshot [u8]),
}
pub type OverlayEntries = Vec<(RowId, Option<Vec<u8>>)>;
struct StagedValue {
    value: Option<Vec<u8>>,
    actor_id: [u8; 16],
}

pub struct Overlay {
    rows: HashMap<RowId, StagedValue>,
    max_rows: usize,
    max_bytes: usize,
    bytes: usize,
    generation: u64,
    aborted: bool,
}
impl Overlay {
    pub fn new(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            rows: HashMap::new(),
            max_rows,
            max_bytes,
            bytes: 0,
            generation: 0,
            aborted: false,
        }
    }

    /// Stage a copied value after checking the overlay's row and value-byte limits.
    pub fn put(&mut self, id: RowId, value: &[u8]) -> Result<(), OverlayError> {
        self.put_as(id, value, [0; 16])
    }

    /// Stage a value attributed to the effective package actor.
    pub fn put_as(
        &mut self,
        id: RowId,
        value: &[u8],
        actor_id: [u8; 16],
    ) -> Result<(), OverlayError> {
        let new_bytes = self.check_change(&id, value.len())?;
        let generation = self.next_generation()?;
        self.rows.insert(
            id,
            StagedValue {
                value: Some(value.to_vec()),
                actor_id,
            },
        );
        self.bytes = new_bytes;
        self.generation = generation;
        Ok(())
    }

    /// Stage a deletion; deletions consume a row slot but no value bytes.
    pub fn delete(&mut self, id: RowId) -> Result<(), OverlayError> {
        self.delete_as(id, [0; 16])
    }

    /// Stage a deletion attributed to the effective package actor.
    pub fn delete_as(&mut self, id: RowId, actor_id: [u8; 16]) -> Result<(), OverlayError> {
        let new_bytes = self.check_change(&id, 0)?;
        let generation = self.next_generation()?;
        self.rows.insert(
            id,
            StagedValue {
                value: None,
                actor_id,
            },
        );
        self.bytes = new_bytes;
        self.generation = generation;
        Ok(())
    }

    /// Remove an invocation-local staged value when a new row is deleted before commit.
    pub fn remove(&mut self, id: &RowId) -> Result<bool, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        if !self.rows.contains_key(id) {
            return Ok(false);
        }
        let generation = self.next_generation()?;
        let Some(old) = self.rows.remove(id) else {
            return Ok(false);
        };
        if let Some(value) = old.value {
            self.bytes -= value.len();
        }
        self.generation = generation;
        Ok(true)
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    fn next_generation(&self) -> Result<u64, OverlayError> {
        self.generation
            .checked_add(1)
            .ok_or(OverlayError::LimitExceeded)
    }

    /// Consume the staged set for a terminal commit attempt.
    pub fn into_entries(self) -> Result<OverlayEntries, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        Ok(self
            .rows
            .into_iter()
            .map(|(id, staged)| (id, staged.value))
            .collect())
    }

    pub fn get(&self, id: &RowId) -> Result<Option<StagedRow<'_>>, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        Ok(self.rows.get(id).map(|staged| match &staged.value {
            Some(bytes) => StagedRow::Value(bytes),
            None => StagedRow::Delete,
        }))
    }

    /// Resolve a staged value only when the current actor authored it. A
    /// different actor's private write is absent from this actor's view, so
    /// reads fall back to the transaction snapshot.
    pub fn get_for_actor(
        &self,
        id: &RowId,
        actor_id: [u8; 16],
    ) -> Result<Option<StagedRow<'_>>, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        let Some(staged) = self.rows.get(id) else {
            return Ok(None);
        };
        if staged.actor_id != actor_id {
            return Ok(None);
        }
        Ok(Some(match &staged.value {
            Some(bytes) => StagedRow::Value(bytes),
            None => StagedRow::Delete,
        }))
    }

    pub fn staged_actor_ids(&self) -> Result<HashSet<[u8; 16]>, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        Ok(self.rows.values().map(|staged| staged.actor_id).collect())
    }

    /// Return an ordered, bounded set of staged row IDs for one table. Values
    /// are resolved later through `read`, preserving overlay ownership rules.
    pub(crate) fn row_ids_after(
        &self,
        table_id: [u8; 16],
        after_row_id: Option<[u8; 16]>,
        limit: usize,
    ) -> Result<(Vec<[u8; 16]>, bool), OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        let mut row_ids = self
            .rows
            .keys()
            .filter(|id| id.table == table_id && after_row_id.is_none_or(|after| id.row > after))
            .map(|id| id.row)
            .collect::<Vec<_>>();
        row_ids.sort_unstable();
        let complete = row_ids.len() < limit;
        row_ids.truncate(limit);
        Ok((row_ids, complete))
    }

    /// Copy all IDs for one table only when the full overlay fits the caller's
    /// bounded-work allowance. Sorting once avoids rescanning the hash map for
    /// every estimate page.
    pub(crate) fn bounded_row_ids_for_table(
        &self,
        table_id: [u8; 16],
        max_overlay_rows: usize,
    ) -> Result<Option<Vec<[u8; 16]>>, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        if self.rows.len() > max_overlay_rows {
            return Ok(None);
        }
        let mut row_ids = self
            .rows
            .keys()
            .filter(|id| id.table == table_id)
            .map(|id| id.row)
            .collect::<Vec<_>>();
        row_ids.sort_unstable();
        Ok(Some(row_ids))
    }

    /// Number of staged entries a bounded table-filtered iteration must visit.
    pub(crate) fn staged_row_count(&self) -> Result<usize, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        Ok(self.rows.len())
    }

    /// Number of bytes in staged row payloads, excluding row IDs and index metadata.
    pub(crate) fn staged_value_bytes(&self) -> Result<usize, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        Ok(self.bytes)
    }

    /// Read the latest staged value first, falling back to the caller's pinned snapshot.
    /// The snapshot lookup is only invoked when this overlay has no entry for the row.
    pub fn read<'overlay, 'snapshot, F>(
        &'overlay self,
        id: &RowId,
        snapshot: F,
    ) -> Result<Option<ResolvedRow<'overlay, 'snapshot>>, OverlayError>
    where
        F: FnOnce(&RowId) -> Option<&'snapshot [u8]>,
    {
        match self.get(id)? {
            Some(StagedRow::Value(value)) => Ok(Some(ResolvedRow::OverlayValue(value))),
            Some(StagedRow::Delete) => Ok(None),
            None => Ok(snapshot(id).map(ResolvedRow::SnapshotValue)),
        }
    }

    pub fn read_for_actor<'overlay, 'snapshot, F>(
        &'overlay self,
        id: &RowId,
        actor_id: [u8; 16],
        snapshot: F,
    ) -> Result<Option<ResolvedRow<'overlay, 'snapshot>>, OverlayError>
    where
        F: FnOnce(&RowId) -> Option<&'snapshot [u8]>,
    {
        match self.get_for_actor(id, actor_id)? {
            Some(StagedRow::Value(value)) => Ok(Some(ResolvedRow::OverlayValue(value))),
            Some(StagedRow::Delete) => Ok(None),
            None => Ok(snapshot(id).map(ResolvedRow::SnapshotValue)),
        }
    }

    /// Discard all staged rows and prevent subsequent use.
    pub fn abort(&mut self) {
        self.rows.clear();
        self.bytes = 0;
        self.aborted = true;
    }

    fn check_change(&mut self, id: &RowId, new_len: usize) -> Result<usize, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        let existing = self.rows.get(id);
        let old_len = existing
            .and_then(|staged| staged.value.as_ref())
            .map_or(0, Vec::len);
        let total = self
            .bytes
            .checked_sub(old_len)
            .and_then(|n| n.checked_add(new_len));
        if (existing.is_none() && self.rows.len() >= self.max_rows)
            || total.is_none_or(|n| n > self.max_bytes)
        {
            self.abort();
            return Err(OverlayError::LimitExceeded);
        }
        Ok(total.expect("checked above"))
    }
}

#[cfg(test)]
mod tests {
    use super::{Overlay, ResolvedRow, RowId, StagedRow};

    fn row() -> RowId {
        RowId {
            table: [1; 16],
            row: [2; 16],
        }
    }

    #[test]
    fn actor_reads_own_staged_value_and_other_actor_snapshot() {
        let mut overlay = Overlay::new(4, 64);
        overlay.put_as(row(), b"uncommitted", [3; 16]).unwrap();

        assert_eq!(
            overlay.get_for_actor(&row(), [3; 16]).unwrap(),
            Some(StagedRow::Value(b"uncommitted"))
        );
        assert!(matches!(
            overlay
                .read_for_actor(&row(), [4; 16], |_| Some(b"snapshot"))
                .unwrap(),
            Some(ResolvedRow::SnapshotValue(b"snapshot"))
        ));
    }

    #[test]
    fn actor_does_not_observe_another_actors_staged_delete() {
        let mut overlay = Overlay::new(4, 64);
        overlay.delete_as(row(), [3; 16]).unwrap();

        assert_eq!(
            overlay.get_for_actor(&row(), [3; 16]).unwrap(),
            Some(StagedRow::Delete)
        );
        assert!(matches!(
            overlay
                .read_for_actor(&row(), [4; 16], |_| Some(b"snapshot"))
                .unwrap(),
            Some(ResolvedRow::SnapshotValue(b"snapshot"))
        ));
    }

    #[test]
    fn staged_writer_ids_are_kept_until_commit_entries_are_created() {
        let mut overlay = Overlay::new(4, 64);
        overlay.put_as(row(), b"value", [3; 16]).unwrap();
        assert_eq!(
            overlay.staged_actor_ids().unwrap(),
            [[3; 16]].into_iter().collect()
        );

        let entries = overlay.into_entries().unwrap();
        assert_eq!(entries, vec![(row(), Some(b"value".to_vec()))]);
    }

    #[test]
    fn write_generation_advances_only_when_the_overlay_changes() {
        let mut overlay = Overlay::new(1, 8);
        assert_eq!(overlay.generation(), 0);
        overlay.put_as(row(), b"one", [3; 16]).unwrap();
        assert_eq!(overlay.generation(), 1);
        overlay.put_as(row(), b"two", [3; 16]).unwrap();
        assert_eq!(overlay.generation(), 2);
        overlay.delete_as(row(), [3; 16]).unwrap();
        assert_eq!(overlay.generation(), 3);
        assert!(
            !overlay
                .remove(&RowId {
                    table: [9; 16],
                    row: [9; 16],
                })
                .unwrap()
        );
        assert_eq!(overlay.generation(), 3);
        assert!(overlay.remove(&row()).unwrap());
        assert_eq!(overlay.generation(), 4);
        assert_eq!(
            overlay.put_as(row(), b"123456789", [3; 16]),
            Err(super::OverlayError::LimitExceeded)
        );
        assert_eq!(overlay.generation(), 4);
    }
}
