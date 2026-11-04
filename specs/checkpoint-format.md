# Checkpoint Format Decision

The engine defines no checkpoint manifest or CHECKPOINT_BEGIN/END WAL records. The pinned [redb backend](redb-storage.md) owns its on-disk roots, allocator state, checkpointing/repair, and recovery format. Do not create a second manifest that can disagree with redb's committed state.

Engine initialization state, catalog version, and required logical metadata live in transactional engine-owned redb tables. The catalog lifecycle checks those values after redb opens and recovers, before enabling admission. Any engine migration is a separate offline, versioned operation using redb's supported transaction API. See [catalog lifecycle](catalog-lifecycle.md).
