# Catalog Payload Encoding v1

## Scope and Physical Container

This is the engine-owned catalog value version 1 for every logical family in the [dictionary](data-dictionary.md). Values are stored in the catalog_v1 redb table defined by [redb storage](redb-storage.md); this is a protected logical encoding, not an on-disk page format or SDK ABI. Maximum envelope size is 1 MiB. redb owns the physical key/value container.

The logical key is stable for a catalog identity. redb read transactions provide old snapshots and atomic replacement at commit; catalog values carry no custom ROW1 version chain. Recreating a dropped named object uses a new object ID. Catalog deletion and dependency retention are engine-level logical operations.

All integers here are little-endian except logical key components defined by [key encoding](index-key-encoding.md). All reserved bytes are zero. redb owns physical checksumming; this envelope validates its own structure and version.

## Envelope and TLVs

| Offset | Bytes | Field |
| --- | --- | --- |
| 0 | 4 | ASCII `CAT1` |
| 4 | 2 | Envelope version=1 |
| 6 | 2 | Family code |
| 8 | 4 | Total envelope bytes |
| 12 | 4 | Logical key byte length |
| 16 | 4 | Field count |
| 20 | 4 | Flags=0 |
| 24 | 8 | Reserved |

The 32-byte header is followed by logical key bytes and then TLVs, with no alignment padding between any items. Each TLV header is `tag u16, wire_type u8, flags u8=0, payload_length u32`, followed by exactly that many bytes. Tags are strictly increasing, unique, and scoped to the family. Reject trailing bytes, missing required tags, unknown tags/types, noncanonical widths, or inconsistent lengths. v1 defines no skippable optional extensions: adding fields requires a catalog payload version change.

Wire types: `U16=1` (2 bytes), `U32=2` (4), `U64=3` (8), `I64=4` (8), `BOOL=5` (one byte 0/1), `ID=6` (16), `TEXT=7` (UTF-8 bytes, no terminator), `BYTES=8`, `IDS=9` (u32 count then exactly count × 16 bytes). IDs are nonzero. Optional fields are omitted, not encoded as a zero ID or null TLV. No field repeats; lists preserve declared order, while sets must be sorted by raw ID bytes and deduplicated.

Bound TEXT to 4,096 bytes (identifier limits may be stricter), IDS to 256 entries unless a lower limit is stated, and individual BYTES to 256 KiB. Larger relationships use child records. Envelope/field counts and arithmetic are checked before allocation; at most 64 fields. Times are signed UTC microseconds since 1970-01-01, not wall-clock strings.

## Family Registry and Canonical Keys

Logical key bytes start with the family code as raw unsigned big-endian u16, followed by the components below using the non-null component encodings in key version 1. ID components use UUID encoding; numeric components use the internal unsigned encodings. Family prefix is not itself a tuple component.

| Code | Family | Key components |
| --- | --- | --- |
| 1 | database | database ID |
| 2 | objects | object ID |
| 3 | schemas | schema ID |
| 4 | types | type ID |
| 5 | tables | table ID |
| 6 | columns | column ID |
| 7 | expressions | expression ID |
| 8 | constraints | constraint ID |
| 9 | indexes | index ID |
| 10 | principals | principal ID |
| 11 | memberships | member ID, role ID |
| 12 | grants | grantee ID, object ID, privilege u16, target-scope u16, target ID |
| 13 | policies | policy ID |
| 14 | policy_targets | policy ID, target-kind u16, target ID |
| 15 | dependencies | dependent ID, referenced ID, edge-kind u16 |
| 16 | credential_verifiers | token ID |

Grant target-scope is object=1 or column=2; object scope repeats object ID as target ID. A grant is one direct edge, not one edge per grantor. Regrant updates provenance on that edge. Fields duplicating key components must agree exactly. Catalog keys use these canonical bytes without a physical version suffix; redb supplies snapshot visibility.

## Field Registry

Notation `tag:name TYPE`; `?` means optional, all other fields are required. IDs embedded only in the key need not be repeated as TLVs. Epochs and schema versions are nonzero u64 values updated under commit serialization. Column-record changes advance the table schema version except nullability-only changes, which preserve the tuple schema version and advance the table write epoch. Semantic constraints from DDL remain mandatory in addition to byte validation.

| Family | Tags |
| --- | --- |
| database | 1:name TEXT; 2:catalog_version U32; 3:builtins BYTES; 4:configuration_id ID?; 5:expression_version U32; 6:builtin_semantics_version U32 |
| objects | 1:kind U16; 2:parent_id ID?; 3:normalized_name TEXT; 4:owner_id ID; 5:definition_epoch U64; 6:state U16 |
| schemas | 1:search_path IDS |
| types | 1:builtin_kind U16; 2:parameter_rule U16; 3:scalar_encoding U32; 4:comparison_version U32; 5:default_mask U32; 6:cast_profile U32 |
| tables | 1:schema_id ID; 2:schema_version U64; 3:write_epoch U64; 4:storage_object_id ID; 5:row_identity_mode U16 |
| columns | 1:table_id ID; 2:ordinal U32; 3:normalized_name TEXT; 4:type_id ID; 5:type_parameters BYTES; 6:nullable BOOL; 7:default_expression_id ID?; 8:generation_mode U16; 9:introduced_schema_version U64; 10:add_column_default_id ID? |
| expressions | 1:purpose U16; 2:ir_version U32; 3:result_type_id ID; 4:result_parameters BYTES; 5:ir BYTES; 6:dependency_ids IDS |
| constraints | 1:table_id ID; 2:kind U16; 3:source_columns IDS; 4:expression_id ID?; 5:referenced_constraint_id ID?; 6:referenced_columns IDS; 7:update_action U16; 8:delete_action U16; 9:match_rule U16; 10:state U16; 11:backing_index_id ID? |
| indexes | 1:table_id ID; 2:columns IDS; 3:method U16; 4:unique BOOL; 5:null_rule U16; 6:owning_constraint_id ID?; 7:storage_object_id ID; 8:state U16; 9:key_version U32; 10:table_schema_version U64 |
| principals | 1:kind U16; 2:normalized_name TEXT; 3:login_enabled BOOL; 4:authority_epoch U64 |
| memberships | 1:grantor_id ID; 2:membership_epoch U64 |
| grants | 1:grantor_id ID; 2:authority_epoch U64 |
| policies | 1:table_id ID; 2:kind U16; 3:operation U16; 4:composition U16; 5:using_expression_id ID?; 6:check_expression_id ID?; 7:authority_epoch U64 |
| policy_targets | No TLVs; key is the entire definition |
| dependencies | No TLVs; key is the entire definition |
| credential_verifiers | 1:user_id ID; 2:algorithm U16; 3:algorithm_version U16; 4:verifier BYTES; 5:issued_at I64; 6:expires_at I64; 7:revoked BOOL; 8:revoked_at I64?; 9:label TEXT; 10:authority_epoch U64 |

Credential verifier profile 1 is SHA-256 (algorithm=1, algorithm_version=1) over an exactly 32-byte uniformly random bearer secret. The stored verifier is exactly 32 bytes. This profile is only for machine-generated high-entropy tokens and must not be used for human-chosen passwords. `issued_at` must be less than `expires_at`; admission accepts only `issued_at <= trusted_now < expires_at` and `revoked=false`. A revoked record requires `revoked_at >= issued_at`; a non-revoked record omits `revoked_at`. Labels are UTF-8 and at most 128 bytes. `authority_epoch` is nonzero and must advance when an existing record changes. A token ID is a nonzero ID distinct from its secret. A verifier must reference a family-10 user; an unrevoked verifier requires that user to have LOGIN enabled. Startup and catalog commits validate the profile and ownership. Unknown algorithms or versions fail closed.

## Enum Values and Structured Fields

Object kinds: schema=1, type=2, table=3, column=4, expression=5, constraint=6, index=7, user=8, role=9, policy=10. Object state is LIVE=1; deletion uses tombstones. Other unknown values fail closed. Principal kind user=1/role=2; roles must have login_enabled=false. The `normalized_name` bytes in a family 10 principal record must equal the bytes in its family 2 object record; the same rule applies to a family 6 column and its object. These are UTF-8 TEXT values and must already be normalized by the trusted DDL path; storage validates byte equality and does not normalize or fold names. Every `owner_id` resolves to a family 10 user or role, and ownership does not grant query privileges. The database record has no parent objects row. At startup, typed records in families 3–9 and 13 must have a family 2 `objects` record with the same ID and the corresponding object kind; family 10 principal kind user/role maps to object kind 8/9 respectively. Catalog commits changing an object, column, or principal validate these name agreements in the final catalog view, so a rename updates both records atomically.

The family 2 object set and typed-record set are complete in both directions. Each object has exactly one same-ID typed row from family 3 for kind 1, 4 for kind 2, 5 for kind 3, 6 for kind 4, 7 for kind 5, 8 for kind 6, 9 for kind 7, 10 for kind 8 or 9, and 13 for kind 10. A family 10 row's principal kind must agree with object kind 8 (user) or 9 (role). Each typed row must also have its same-ID family 2 object with the corresponding kind. Startup checks the full catalog; catalog commits check the final view for every object ID whose object or typed row changes, so creation, update, and deletion publish both sides atomically.

Built-in types: BOOLEAN=1, SMALLINT=2, INTEGER=3, BIGINT=4, DECIMAL=5, TEXT=6, VARCHAR=7, BYTEA=8, UUID=9, DATE=10, TIMESTAMPTZ=11. There is exactly one family 4 row for each built-in kind. Type parameter-rule codes: none=0, decimal=1, varchar=2. `type_parameters` and `result_parameters` are exactly eight bytes: precision u16, scale u16, maximum character count u32. Unused fields are zero. Validate decimal p/s and nonzero VARCHAR count against DDL. The persisted family 4 row for each built-in kind must use its assigned parameter rule (DECIMAL=1, VARCHAR=2, all others=0), scalar encoding 1, comparison version 1, cast profile 1, and default mask 1 for all types except UUID, whose mask is 7. Startup and catalog commits reject missing, duplicate, or modified built-in type profiles. Cast profile 1 permits only the exact conversions supported by the typed SQL contract; the field does not authorize additional conversions.

Type default_mask bits: literal=bit0, generated-UUID=bit1, current-principal=bit2; no other bits. All initial types permit literal defaults, only UUID permits bits1/2. Column generation mode none=0/generated UUID=1; table row_identity_mode=1 (opaque UUID). Ordinals are zero-based positions in the identified table schema; old definitions remain accessible by schema version. `add_column_default_id` is immutable after introduction and refers to a literal; absence means null for an absent old-record field. Current insert defaults may change independently.

Expression purpose: default=1, check=2, policy_using=3, policy_check=4. `ir_version=1` selects EIR1, a canonical prefix encoding beginning with `EIR1`, u16 LE version 1, and two zero flag bytes. The remaining bytes are prefix nodes: each opcode is followed by its fixed operands and then its child node(s). Node opcodes are literal=1, field=2, current-principal=3, has-role=4, equal=16, not-equal=17, less=18, less-equal=19, greater=20, greater-equal=21, and=22, or=23, not=24, is-null=25. Field has a u16 LE ordinal; HasRole has a nonzero 16-byte ID; And/Or have a u16 LE child count followed by that many nodes. Binary comparisons have two nodes, Not/IsNull have one, and other operators have no child nodes.

Literal nodes contain one scalar tag: NULL=0 (no bytes), BOOLEAN=1 (one canonical 0/1 byte), SMALLINT=2 (i16 LE), INTEGER=3 (i32 LE), BIGINT=4 (i64 LE), DECIMAL=5 (i128 LE coefficient at scale zero), TEXT=6 (u32 LE byte length plus valid UTF-8), VARCHAR=7 (u32 LE byte length plus valid UTF-8), BYTEA=8 (u32 LE byte length plus bytes), UUID=9 (16 bytes), DATE=10 (i32 LE days), TIMESTAMPTZ=11 (i64 LE UTC microseconds). Scalar values must also pass the v1 tuple scalar range/precision rules. Empty AND evaluates TRUE and empty OR evaluates FALSE. The whole EIR1 value is at most 256 KiB, 256 nodes and 32 levels deep; field ordinals are 0–4095. Decoders reject unknown opcodes/tags, truncated/trailing bytes, invalid scalar encodings, and work-limit overflow. EIR1 decoding establishes structural validity; before execution, DDL and invocation paths bind the expression purpose and result type, check Boolean operands for Boolean operators, and require comparison operands to have the same SQL scalar type (Decimal scales must match; VARCHAR length parameters may differ). NULL is compatible with any operand type and retains SQL unknown semantics. Field ordinals bind to the table's ordered column IDs; HasRole binds to a role ID; the discovered stable IDs must equal `dependency_ids` exactly. Defaults are restricted to a root literal or the admitted principal ID. No implicit functions, casts, I/O, or callbacks exist. Unsupported IR versions fail closed. Credential verifiers use only the profile defined above; BYTES is not permission to accept arbitrary verifier algorithms.

Constraint kinds: not_null=1, primary_key=2, unique=3, check=4, foreign_key=5. State VALID=1; BUILDING is private and never a committed usable definition. Every constraint's `table_id` resolves to a family 5 table/object kind 3, and each source column resolves to a family 6 column/object kind 4 attached to that table. PK/unique require nonempty source columns and backing index; FK requires matching ordered source/referenced columns and referenced PK/unique constraint; CHECK requires expression and an empty source list (expression dependencies hold its columns); NOT NULL has exactly one source column and that column must be non-nullable. Irrelevant optional IDs are absent and irrelevant lists empty. Actions none=0/RESTRICT=1; match none=0/SIMPLE=1; FK uses 1 for all three, other constraints use 0. Index method BTREE=1, null_rule=NULLS_DISTINCT=1, state VALID=1, key_version=1. Index columns are nonempty, ordered, and distinct. Every index table ID resolves to a family 5 table with object kind 3; each ordered column ID resolves to a family 6 column with object kind 4 attached to that same table. Startup and catalog commit reject missing or cross-table index references. An FK's ordered `referenced_columns` must equal the ordered `source_columns` of its referenced PRIMARY KEY or UNIQUE constraint; both column lists must belong to their respective constraint tables.

Privileges: USAGE=1, SELECT=2, INSERT=3, UPDATE=4, DELETE=5, REFERENCES=6, CREATE=7, ALTER=8, INDEX=9, MANAGE_POLICIES=10, MANAGE_GRANTS=11, MANAGE_USERS=12, MANAGE_ROLES=13, CREATE_SCHEMA=14. USAGE and CREATE target a schema; SELECT, INSERT, UPDATE, and REFERENCES target a table or one of its columns; DELETE targets a table; ALTER, INDEX, MANAGE_POLICIES, and MANAGE_GRANTS target a schema or table; MANAGE_USERS, MANAGE_ROLES, and CREATE_SCHEMA target the database record. Object scope repeats `object_id` in `target_id`; column scope names the table in `object_id` and one of its family 6 columns in `target_id`. No ALL or grant-option bit exists.

Policy kind row=1/cell=2; operation SELECT=1/INSERT=2/UPDATE=3/DELETE=4; composition ALLOW=1/RESTRICT=2. Clause requirements follow permission DDL. Target kind recipient-user=1/recipient-role=2/column=3. Every policy requires recipients; cell policies require column targets, row policies forbid them. A column target is valid only for a cell policy and must identify a family 6 column whose `table_id` equals the policy's family 13 `table_id`; that table must have a family 5 record and object kind 3. The target column must have a family 6 record and object kind 4. Dependencies: owned-child=1, expression-reference=2, foreign-key-target=3, backing-index=4, authority=5. The edge goes from dependent to referenced; owned-child points child to parent and must exactly match the child's `objects.parent_id`. A child has at most one owned-child parent; parent cycles fail validation. One generic ID may appear under different edge kinds. A kind-2 edge has one of two shapes: (a) a family 6 column with `default_expression_id` or `add_column_default_id`, a family 8 CHECK constraint with `expression_id`, or a family 13 policy with `using_expression_id`/`check_expression_id` points to the referenced family 7 expression; (b) a family 7 expression points to each family 6 column or family 10 role ID in its `dependency_ids`. `add_column_default_id` points to a literal-root family 7 expression. Every referenced expression, column, and role must have its matching typed record and object kind. The set of kind-2 edges must equal the pairs implied by these fields and lists; absent, extra, wrong-target, and duplicate expression dependency IDs are invalid. A CHECK constraint may carry `expression_id` only when its kind is CHECK. A kind-3 edge goes from a family 8 FOREIGN KEY constraint object to the referenced family 8 PRIMARY KEY or UNIQUE constraint object. It exactly mirrors `referenced_constraint_id`: every FK has one edge to that ID, and no non-FK constraint has a kind-3 edge. A kind-4 edge goes from a family 8 PRIMARY KEY or UNIQUE constraint object to its family 9 backing index object. It exactly mirrors `backing_index_id` and `indexes.owning_constraint_id`; each PK/UNIQUE constraint has one edge, and an index with an owning constraint has the inverse matching edge.

Kind-5 authority edges are the deduplicated dependent→referenced pairs implied by authority records: family 11 membership maps `member_id`→`role_id`; family 12 grants map `grantee_id`→`target_id`; family 14 recipient-user/recipient-role rows map `policy_id`→recipient principal. Column-kind policy targets do not create kind-5 edges. Grantor IDs are provenance and are validated as principals, without a kind-5 edge. The complete set of kind-5 edges must equal these implied pairs; missing and extra edges are invalid. A database-scoped system grant points to the family 1 database ID, which is permitted as a referenced endpoint only for kind 5. All other dependency endpoints resolve to family 2 objects. `objects.owner_id` is checked directly against family 10 principals and does not imply a kind-5 edge.

## Bootstrap Mapping and Validation

The database builtins field is `count u32`, then sorted entries `(code u16, reserved u16=0, ID[16])`, maximum 256 entries. Required codes: system schema=1, information_schema=2, database_admin role=3; primitive type codes are `100 + builtin_kind`; catalog family storage-object codes are `200 + family_code`. For this descriptor version, startup requires exactly those 30 codes; missing, extra, unknown, unsorted, duplicate, zero-ID, or nonzero-reserved entries fail closed until the descriptor is extended. Startup and catalog commits also resolve codes 1 and 2 to family 3 schema records and family 2 objects of kind 1; code 3 to a family 10 role principal and family 2 object of kind 9, both named `database_admin`; and codes 101–111 to family 4 primitive records whose `builtin_kind` equals `code - 100` and family 2 objects of kind 2. Codes 201–216 currently identify opaque per-family storage objects and have no catalog record target to resolve; their IDs are checked for descriptor shape, nonzero value, and uniqueness only. IDs are database-local opaque generated values persisted at initialization; reserved meaning is determined by code, not UUID bits. Engine descriptors know the codes/record schema before reading SQL metadata. The redb table names and built-in descriptor locate logical catalog records without relying on SQL queries.

redb holds catalog records in the catalog_v1 logical table. The engine may add secondary logical indexes as redb tables and must update them in the same redb transaction. It has no catalog-directory B-tree of physical roots; lookup keys are family prefix plus the canonical logical key.

Validate required mappings, uniqueness, ownership, named-object lifetimes, graph references/cycles, policy targets, epoch coherence, and current schema compatibility before publication/startup. DDL rejection is atomic. Dropping a required built-in is forbidden. Verifier bytes and expression/policy bodies never pass through ordinary metadata discovery.

## Fixtures and Remaining Boundaries

A policy_targets record with recipient-user target has family 14, key length 39 (`00 0e`, UUID component 17, U16 component 3, UUID component 17), field count 0, total length 71. Its header begins `43 41 54 31 01 00 0e 00 47 00 00 00 27 00 00 00`, followed by sixteen zero bytes and then the key. A BOOL TLV with tag 6 and true is `06 00 05 00 01 00 00 00 01`.

Create binary fixtures for every family, absent optional fields, empty ordered lists, unknown tags, duplicate/out-of-order fields, corrupted graph references, tombstones, and old-schema decoding. Physical allocation, outcome/audit payloads, and executable expression IR remain separate contracts.
