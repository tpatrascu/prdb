//! Bounded invocation-local staged row values. This module does not authorize writes.
use std::collections::HashMap;

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
pub struct Overlay {
    rows: HashMap<RowId, Option<Vec<u8>>>,
    max_rows: usize,
    max_bytes: usize,
    bytes: usize,
    aborted: bool,
}
impl Overlay {
    pub fn new(max_rows: usize, max_bytes: usize) -> Self {
        Self {
            rows: HashMap::new(),
            max_rows,
            max_bytes,
            bytes: 0,
            aborted: false,
        }
    }

    /// Stage a copied value after checking the overlay's row and value-byte limits.
    pub fn put(&mut self, id: RowId, value: &[u8]) -> Result<(), OverlayError> {
        let new_bytes = self.check_change(&id, value.len())?;
        self.rows.insert(id, Some(value.to_vec()));
        self.bytes = new_bytes;
        Ok(())
    }

    /// Stage a deletion; deletions consume a row slot but no value bytes.
    pub fn delete(&mut self, id: RowId) -> Result<(), OverlayError> {
        let new_bytes = self.check_change(&id, 0)?;
        self.rows.insert(id, None);
        self.bytes = new_bytes;
        Ok(())
    }

    /// Remove an invocation-local staged value when a new row is deleted before commit.
    pub fn remove(&mut self, id: &RowId) -> Result<bool, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        let Some(old) = self.rows.remove(id) else {
            return Ok(false);
        };
        if let Some(value) = old {
            self.bytes -= value.len();
        }
        Ok(true)
    }

    /// Consume the staged set for a terminal commit attempt.
    pub fn into_entries(self) -> Result<OverlayEntries, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        Ok(self.rows.into_iter().collect())
    }

    pub fn get(&self, id: &RowId) -> Result<Option<StagedRow<'_>>, OverlayError> {
        if self.aborted {
            return Err(OverlayError::Aborted);
        }
        Ok(self.rows.get(id).map(|v| match v {
            Some(bytes) => StagedRow::Value(bytes),
            None => StagedRow::Delete,
        }))
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
        let old_len = existing.and_then(Option::as_ref).map_or(0, Vec::len);
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
