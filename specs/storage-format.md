# Storage Format Decision: redb

The engine delegates physical database-file format, pages, B+trees, allocation, copy-on-write, reclamation, and transaction recovery to the pinned [redb backend](redb-storage.md). It defines no custom page size, row slot, heap/overflow page, root slot, or physical file layout. Never parse or mutate redb file bytes from engine code.

Engine-owned values remain explicitly serialized, versioned key/value records: see [catalog payloads](catalog-payloads.md) and [redb table mapping](redb-storage.md). [Index-key encoding](index-key-encoding.md) defines SQL logical order only where required by the chosen redb API. Its length and comparator assumptions must be reconciled with the pinned release before they become a public compatibility promise.

redb format version and engine catalog/value version are independent. Pin and validate a specific crate release, use its supported open/upgrade path, and fail closed on unsupported engine metadata. There is no custom-format migration requirement for the unimplemented design.
