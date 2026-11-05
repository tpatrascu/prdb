# Data Dictionary Progress

## Policy-target catalog envelope — in progress

- Contract: [catalog payloads v1](../../specs/catalog-payloads.md), family 14. The 71-byte envelope includes a 39-byte canonical logical key and no TLVs; IDs are nonzero and target kind is 1, 2, or 3.
- Both tests failed when the compiling placeholder rejected every record. The implementation now encodes the independently specified fixture and rejects malformed header, key, marker, ID, kind, length and trailing bytes.
- This validates only the record's bytes and local fields. Catalog graph references, policy semantics, storage, bootstrap, and all other catalog families remain unimplemented.

## DD-1: schema-driven CAT1 envelope — in progress

- Contract: [catalog payload encoding v1](../../specs/catalog-payloads.md), Envelope and TLVs. Added a shared schema-driven envelope encoder/decoder with explicit family field registries, required/optional tags, typed wire values, canonical sorted TLVs, exact lengths, zero reserved bytes, size/field/list bounds, and rejection of malformed IDs, invalid UTF-8, and unknown tags/types. IDS fields validate nonzero IDs and can declare sorted-unique set semantics.
- Red/green evidence: generic round-trip, unknown-field, malformed-length, wire-width, boolean, UTF-8, and IDS set fixtures pass; the pre-existing 71-byte policy-target binary fixture remains byte-identical.
- Verification: the direct real-redb-linked engine suite passed all 55 integration tests across 14 files; direct rustfmt check and direct Clippy warnings-as-errors passed. A pinned redb snapshot test reads a catalog payload, while malformed stored catalog bytes fail closed. Catalog/audit/outcome publication is covered by a real-redb atomic-commit test.
- Added concrete field registries for all 16 catalog families and canonical family-key structure checks, including typed UUID/u16 components, bounded enum codes, and the object-scope grant target equality rule. A table-driven fixture now encodes and decodes required fields for every registered family.
- Membership graph integrity is now checked against the latest catalog plus staged membership changes during serialized publication. It rejects missing member/role records, non-role targets, disconnected cycles, and graphs over 4096 edges. Real-redb tests verify rollback and ABORTED outcome publication.
- Family-7 expression records now use bounded EIR1 and are validated on catalog reads/writes. Invocation APIs load purpose-checked default/CHECK expressions from their pinned snapshot and guard their bytes through final commit.
- Remaining: generated all-family fixtures are not independent hand-checked byte fixtures; broader semantic cross-record integrity, bootstrap, DDL validation/authority checks, catalog secondary indexes, and comprehensive snapshot-aware catalog operations remain unimplemented.
