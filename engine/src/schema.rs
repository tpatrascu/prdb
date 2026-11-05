//! Versioned redb table names and byte-oriented engine-private key/value types.
use redb::{TableDefinition, WriteTransaction};

pub const ENGINE_META: TableDefinition<&[u8], &[u8]> = TableDefinition::new("engine_meta_v1");
pub const ROWS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("rows_v1");
pub const INDEXES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("indexes_v1");
/// Reverse memberships make all index entries for one changed row addressable by row prefix.
pub const ROW_INDEXES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("row_indexes_v1");
pub const CATALOG: TableDefinition<&[u8], &[u8]> = TableDefinition::new("catalog_v1");
pub const OUTCOMES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("outcomes_v1");
pub const AUDIT: TableDefinition<&[u8], &[u8]> = TableDefinition::new("audit_v1");
pub const STATS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("stats_v1");

/// Create every logical engine table in the caller's redb write transaction.
/// The caller must commit or abort this transaction as one atomic unit.
pub fn create_tables(write: &WriteTransaction) -> Result<(), Box<dyn std::error::Error>> {
    drop(write.open_table(ENGINE_META)?);
    drop(write.open_table(ROWS)?);
    drop(write.open_table(INDEXES)?);
    drop(write.open_table(ROW_INDEXES)?);
    drop(write.open_table(CATALOG)?);
    drop(write.open_table(OUTCOMES)?);
    drop(write.open_table(AUDIT)?);
    drop(write.open_table(STATS)?);
    Ok(())
}
