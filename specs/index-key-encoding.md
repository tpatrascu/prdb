# Index-Key Encoding v1

## Contract

This document fixes key encoding version 1 for the [B-tree page format](storage-format.md). Compare keys by unsigned lexicographic byte order, shorter prefix first. There is no locale collation, descending direction, prefix compression, or floating-point key type in v1. Payload integers elsewhere remain little-endian; ordered key integers deliberately use big-endian.

The index's pinned catalog definition determines component count, types, precision/scale, and comparison version. Keys contain no type tags or component count. Never compare keys from different definitions. Changing key semantics requires an explicit version/format migration and index rebuild, not reinterpretation of existing bytes.

## SQL Tuple Components

Encode components in declared column order, concatenated without padding:

- SQL NULL: byte `ff`, with no following payload.
- Non-null: byte `01` followed by the type payload below.
- Bytes `00` and `02`–`fe` are invalid component markers. REDACTED is never an index value.

| Type | Non-null payload |
| --- | --- |
| BOOLEAN | One byte `00` for false or `01` for true |
| SMALLINT / INTEGER / BIGINT | Signed 16/32/64-bit value: take its two's-complement bits, XOR the most significant bit, emit the fixed-width result big-endian |
| DECIMAL(p,s) | Signed 128-bit unscaled coefficient, sign-normalized as above, 16 bytes big-endian |
| DATE | Signed i32 day count, sign-normalized, 4 bytes big-endian |
| TIMESTAMP WITH TIME ZONE | Signed i64 UTC microseconds, sign-normalized, 8 bytes big-endian |
| UUID | The canonical opaque 16 bytes used in storage, without host-endian conversion |
| TEXT / VARCHAR | Validated UTF-8 bytes with the escaping below; binary comparison, no normalization or space trimming |
| BYTEA | Arbitrary bytes with the same escaping |

For variable-width payloads, replace each input `00` with `00 ff`; copy every other byte unchanged; terminate with `00 00`. No other sequence beginning with `00` is valid. This preserves byte order, distinguishes a value from its longer extension, and makes tuple boundaries unambiguous. An empty value has payload `00 00`. Validate UTF-8 and VARCHAR length before encoding text; BYTEA has no text validation.

Decimal scale is fixed by the indexed column. Coerce to that declared scale using the SQL exact-conversion rules before encoding; reject overflow or inexact conversion. For DECIMAL(5,2), 1.20 has coefficient 120. Comparison across independently scaled indexes is undefined; query literals must first be coerced to the indexed type. No per-value scale byte is stored.

## Complete Keys and Values

**Internal row directory:** key is raw row UUID (16 bytes), with no component marker or suffix. Leaf value is the 40-byte latest-version reference. This directory is not a SQL primary-key index.

**SQL secondary indexes, including primary/unique backing indexes:** key is the SQL tuple followed by raw row UUID (16 bytes), then begin-commit sequence as unsigned big-endian u64 (8 bytes). Leaf value is the 40-byte reference to that row version. The suffix is always present, including for unique indexes. A live physical key therefore uniquely identifies a version; reject duplicate complete keys rather than silently overwriting another version.

The former 2,048-byte limit and 24-byte suffix came from the retired custom B-tree format and do not apply. Set a logical maximum compatible with the pinned redb Key/value API before implementation; reject oversized values atomically.

Historical entries remain until MVCC reclamation permits removal. Reconstruct their tuple from the referenced version, never from a newer row. Validate visibility, row/cell policies, and private overlays after candidate lookup. Updating only an unindexed column still creates the new version's index entry under this baseline.

Uniqueness compares the decoded user tuple, excluding the suffix, against relevant current committed rows and the final private write set. Any null component exempts a UNIQUE tuple from conflict; PRIMARY KEY rejects null first. Old/dead versions do not conflict merely because their encoded prefix matches. The engine performs the final constraint check inside the serialized redb write transaction.

**Catalog indexes:** [catalog payloads](catalog-payloads.md) defines canonical logical key bytes. Catalog keys are the family prefix and canonical logical key; redb read transactions provide snapshot visibility. Catalog secondary lookups use typed components defined by their table schema. Catalog component unsigned u16/u32/u64 values use marker `01` plus fixed-width unsigned big-endian bytes. These unsigned component types are internal, not additional SQL types. No custom physical catalog-directory tree is defined.

## Routing and Search Bounds

Internal-page separators are complete copies of the smallest key in their right child at the split. The leftmost reference covers keys below the first separator; each separator routes equality and greater keys to its right child until the next separator. No separator shortening or synthetic persisted key is permitted. Deletion may leave a conservative separator below the right child's new minimum only while its partition remains valid; merges/repartitioning must update separators atomically. Sibling links are not a substitute for validating the search path.

Range bounds are search objects, not persisted keys. For a complete non-null/null tuple prefix P, equality probes cover all keys beginning with P, including their suffixes. Use a prefix-aware comparator or `[P, successor(P))`, where successor increments the last non-`ff` byte and drops following bytes. If no such byte exists, the upper bound is unbounded. Never decode the successor as a stored key. Composite-prefix ranges must end at a component boundary; SQL range/null semantics are applied separately.

## Normative Encoding Examples

Spaces separate bytes; these are component/tuple bytes before the secondary suffix unless noted.

| Input | Hex bytes |
| --- | --- |
| SMALLINT minimum / -1 / 0 / maximum | `01 00 00` / `01 7f ff` / `01 80 00` / `01 ff ff` |
| INTEGER -1 / 0 / 1 | `01 7f ff ff ff` / `01 80 00 00 00` / `01 80 00 00 01` |
| false / true / NULL | `01 00` / `01 01` / `ff` |
| Empty TEXT | `01 00 00` |
| TEXT `A` | `01 41 00 00` |
| BYTEA bytes `41 00 42` | `01 41 00 ff 42 00 00` |
| TEXT `é` (U+00E9) | `01 c3 a9 00 00` |
| Tuple (INTEGER 1, TEXT `A`, NULL) | `01 80 00 00 01 01 41 00 00 ff` |

DECIMAL(5,2) 1.20 encodes as marker `01`, payload `80`, fourteen `00` bytes, then `78`. -1.20 encodes as marker `01`, payload `7f`, fourteen `ff` bytes, then `88`. DATE epoch day and timestamp epoch use their respective integer-zero encodings. A suffix with row UUID bytes `00` repeated fifteen times then `01`, and begin commit 2, is those 16 UUID bytes followed by `00 00 00 00 00 00 00 02`.

## Required Evidence

Create checked binary fixtures from the examples and independent typed-comparator properties. Cover extremes, embedded zero/ff, empty and prefix strings, Unicode byte order, nulls in each tuple position, decimal signs, suffix ordering, exact length limits, malformed escapes, truncated fixed-width components, invalid booleans, and trailing garbage. Compare index scans with reference scans for results and authorization errors, not only ordering. These examples specify expected bytes; no executable codec or passing runtime test is claimed here.
