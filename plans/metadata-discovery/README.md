# Metadata Discovery Implementation Plan

## Contract and Dependencies

Implement [filtered discovery](../../specs/metadata-discovery.md) in storage, using [dictionary](../data-dictionary/README.md), [authorization](../authorization/README.md) and scoped [IPC](../ipc-protocol/README.md). [SQL](../sql-library/README.md) and [optimizer](../planner-compiler/README.md) consume these projections; neither reads raw catalogs. Live runtime state belongs to a separate control surface.

## Decisions Before Implementation

Specify bounded typed records, deterministic ordering, byte/page limits and opaque continuation-token encoding/lifetime. Tokens bind connection, role, query/filter and snapshot. Fix hidden/nonexistent error equivalence and privilege-filtered administrative sections; ordinary metadata cannot leak defaults, policy text, owners, grant provenance, physical counts/paths or global epochs.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| MD-1 | Scope/schema USAGE and an applicable data grant control discovery without probing table rows | list_schemas/list_relations/describe_columns over pinned catalog/policy context; write-only grants can discover declared shape |
| MD-2 | Hidden/nonexistent names and populated/empty hidden rows give equivalent ordinary discovery | Storage filtering before encoding, visible object IDs as names only, no hidden totals or relation ordinal gaps |
| MD-3 | Substituted/expired/cross-principal tokens reject; paging preserves old snapshot through rename | Bounded deterministic pages, snapshot-bound ownership and independently authorized relation resolution on each connection |
| MD-4 | API and SQL schemata/tables/columns contents agree under predicates and LIMIT | Three reserved read-only information_schema virtual scans, fixed public self-description and no recursive bootstrap dependency |
| MD-5 | Mutations/grants/policy edits on virtual relations reject; optimizer sees only approved logical access paths | Dedicated system interface and role-filtered projection without raw definition/private statistics access |
| MD-6 | Admin revocation between pages invalidates cursor; dependency inspection does not expose hidden endpoints | Request-scoped catalog snapshot plus current administrative authority, bounded sections and no secret/verifier access even for admins |

## Current Implementation Slice

`InvocationTransaction::load_catalog_schema_usage_grants` derives the admitted principal's schema USAGE grants and pinned role closure from guarded catalog records. `list_catalog_authorized_schemas` uses those records to construct the discovery context internally for the admitted invocation. `InvocationTransaction::list_authorized_schemas` implements USAGE-filtered schema listing in pages of at most 128 schemas and 1 MiB, ordered by stable schema ID; `resolve_authorized_schema` performs exact-name lookup over those pages without granting authority. Schema, relation, and column cursors have fixed 16-byte opaque wire tokens backed by invocation-local one-use state. `InvocationTransaction::load_catalog_relation_discovery_grants` derives schema USAGE and table/column data grants for one schema; `list_catalog_authorized_relations` uses these pinned grants and the invocation principal to construct its context internally. `InvocationTransaction::list_authorized_relations` implements the first MD-1 relation behavior: schema USAGE plus a table or column data grant controls listing, including write-only grants, without consulting table rows or row policies. Its pages are bounded to 128 relations and 1 MiB, ordered by stable relation ID, and use invocation-local continuations. `InvocationTransaction::describe_authorized_columns` returns the full declared shape of one admitted relation in a single response capped at 1 MiB; `describe_authorized_columns_page` returns ordinal-ordered pages capped at 128 columns and 1 MiB. `InvocationTransaction::resolve_authorized_relation` walks filtered pages by exact name and independently rechecks authority before returning the visible relation's declared shape. `resolve_catalog_authorized_relation` composes schema and relation lookup using pinned catalog grants. These results do not authorize row reads. Cursors across process restart, built-in `information_schema` scans, and administrative discovery remain unimplemented.

## Acceptance and Handoff

Run real two-user API/SQL parity fixtures, continuation substitution, empty/hidden data variation and old-reader DDL tests. Discovery never grants data access or permits planner-handle reuse by executors. First behavior: list one granted relation and omit an inaccessible one without consulting row visibility.
