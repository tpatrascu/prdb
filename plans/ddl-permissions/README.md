# Permission DDL Implementation Plan

## Contract and Dependencies

Implement [grants and mandatory policies](../../specs/ddl-permissions.md) through authenticated administrative transactions. Depend on [dictionary](../data-dictionary/README.md), [identity](../ddl-identity/README.md), [schema definitions](../ddl-schema/README.md) and [runtime authorization](../authorization/README.md). This plan owns definition validation/publication; runtime enforcement is tested jointly with authorization.

## Decisions Before Implementation

Specify bounded permission syntax/structured operations and typed policy IR shared with trusted expressions. Bind names to stable IDs and record recipients, columns, trusted context functions and dependencies. Freeze limits/error ordering; no arbitrary native code, external inputs, subqueries, caller-writable session authority, PUBLIC/ALL/grant option or implicit bypass.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| DP-1 | Unauthorized grant/policy change fails independently of a valid parse; owner queries still deny data | Validate MANAGE_GRANTS/MANAGE_POLICIES/object ownership and database-admin-only system privilege changes on control channels |
| DP-2 | Table-wide grants expand only to existing columns; revoking one direct source leaves inherited sources intact | Explicit field target records for SELECT/INSERT/UPDATE/REFERENCES, schema USAGE and object privileges, atomic provenance/epoch changes |
| DP-3 | Missing operation clauses, wrong target/column kinds or duplicate table policy names reject | CREATE/DROP ROW/CELL policy validation, SELECT/DELETE USING, INSERT CHECK, UPDATE both; no DELETE cell policy |
| DP-4 | Typed bounded expressions bind principal/role/column IDs and reject unsafe functions or invalid dependencies | Trusted CURRENT_PRINCIPAL_ID/HAS_ROLE, immutable resolved references, fail-closed null/runtime errors and policy-driving field dependencies |
| DP-5 | Atomic drop/create replacement has no missing-policy window; concurrent privilege revocation aborts stale admin commit | Publish definitions/dependencies/authority epochs/audit/outcome in one redb transaction with current actor revalidation |
| DP-6 | Real manager fixture reveals mixed salaries and denies mismatched old/new ALLOW pairs, defaults and self-escalation | End-to-end runtime tests for mandatory composition, per-column policies, generated identity exception, immutable policy inputs and bounded snapshot revocation |
| DP-7 | A newly added policy-driving field freezes future query updates; inaccessible policy text stays private | Coordinate schema/write restrictions, filtered discovery and privileged-only provenance/definition inspection |

## Acceptance and Handoff

Cover default denial, multiple roles/RESTRICT, new columns without grants, row visibility versus cell readability and stale writes after policy/membership changes. Use real control-created identities/tables and hostile direct storage requests to demonstrate SDK independence. First behavior: authorize and persist one direct field grant with a denied administrative counterpart; grant alone must still fail without mandatory policies.
