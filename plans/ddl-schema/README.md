# Schema DDL Implementation Plan

## Contract and Dependencies

Implement [schema/type/table/index/constraint administration](../../specs/ddl-schema.md) through the authenticated control path. Depend on [dictionary](../data-dictionary/README.md), [storage/index codecs](../storage-manager/README.md), [transactions](../concurrency-mvcc/README.md), [identity](../ddl-identity/README.md) and [permissions](../ddl-permissions/README.md). Query invocations reject DDL even for administrative principals.

## Decisions Before Implementation

Fix bounded DDL text/structured schemas, normalized identifier limits, scalar wire/storage ranges, exact decimal casts, UTC date/timestamp ranges and trusted default/CHECK IR. Preserve the declared initial scalar set, constraints and supported ALTER forms. Define deterministic fence acquisition, affected-table schema/write epochs and private-build publication/cleanup without custom physical trees.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| DS-1 | Qualified/unquoted/quoted names bind predictably; unsupported definitions and unauthorized actors fail | Independent trusted validation, admitted default schema for client names, reserved namespaces and current actor checks |
| DS-2 | All declared scalar/default fixtures round-trip exactly; invalid Unicode/count/precision/overflow rejects atomically | BOOLEAN, signed integers, DECIMAL up to precision 38, TEXT/VARCHAR/BYTEA/UUID/DATE/TIMESTAMPTZ codecs and checked conversions; trusted generated identity/current-principal defaults |
| DS-3 | Create table publishes catalog/storage/constraints atomically with audit/outcome and denies implicit data access | Minimal schema/table creation then full PK/UNIQUE/NOT NULL/CHECK/non-cascading FK definitions, stable IDs and typed dependency records |
| DS-4 | Simultaneous duplicate keys or parent/child races leave constraints valid and loser fully aborted | Final uniqueness and both FK directions over latest state plus final overlay; MATCH SIMPLE, NULLS DISTINCT and CHECK-unknown semantics; REFERENCES authority required |
| DS-5 | Rename/add/default/NOT NULL/constraint/ownership/drop operations preserve stable references and old-row meaning | Implement every supported ALTER incrementally; immutable add-column defaults, restricted drops and no new-column grants; reject type rewrite/cascades and deferred features |
| DS-6 | Index versus reference scan agrees on visibility/errors; failed/crashed build exposes no usable partial index | Plain-column ascending nulls-last logical indexes with redb ordering fixtures, constraint-owned index restrictions and atomic VALID publication |
| DS-7 | DDL fences block affected commits while unrelated writes/old readers continue; stale writers abort | Acquire ordered fences, capture latest build snapshot after fencing, avoid long redb writer holds, validate/publish atomically and retain old definitions/indexes until safe |

## Acceptance and Handoff

Use actual administrative prepare/start/status operations and real redb data. Cover default changes versus backfill, duplicate/existence checks after authority, graph races, hidden constraint diagnostics, old readers after rename/drop and crash/reopen during publication. Initial scope is incomplete without unique/FK constraints and the supported DDL forms. First behavior: one authorized schema/table creation and a denied control request, once minimal identity/catalog plumbing exists.
