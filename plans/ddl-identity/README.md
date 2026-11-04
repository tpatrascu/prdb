# Identity and Authentication Implementation Plan

## Contract and Dependencies

Implement [users/roles/credentials](../../specs/ddl-identity.md) within trusted storage/control admission. Depend on [dictionary](../data-dictionary/README.md), [bootstrap](../catalog-lifecycle/README.md), [transaction epochs](../concurrency-mvcc/README.md) and [audit](../operational-metadata/README.md). Minimal bootstrap records and token validation precede the complete DDL surface.

## Decisions Before Implementation

Specify cryptographic algorithm/version codes, secure randomness, verifier format/length, expiry comparison, generic failure handling and rate limits before issuing tokens. Define client secret storage, protected setup delivery and secure secret handling/zeroization policy. Distinguish database users/roles from IPC service roles. Record offline administrator recovery and deliberate expiry/rotation policy separately from normal startup.

## Increments

| ID | Observable test first | Minimum implementation and evidence |
| --- | --- | --- |
| ID-1 | Protected bootstrap produces one usable secret, stores only its verifier and cannot repeat on READY | Integrate INITIALIZING handoff/replacement with lifecycle; no default password, anonymous administrator or OS-UID identity shortcut |
| ID-2 | Wrong/expired/revoked/disabled credentials fail generically; request principal IDs never authenticate | Trusted token verification, storage-bound sessions and revalidation at each new execution/admin/status/cancel operation |
| ID-3 | User/role shared names, stable rename IDs, transitive inheritance and cycle/self-edge rejection hold | Authorized CREATE/ALTER/DROP and membership edits with dependency checks, deterministic bounded role closure and atomic epochs/audit/outcome |
| ID-4 | Issue/list/revoke/rotate returns secret once, never verifier/plaintext in logs or catalog inspection | Separate credential control APIs with MANAGE_USERS checks, login-enabled targets, nonsecret inventories and lost-delivery revoke/reissue |
| ID-5 | NOLOGIN revokes tokens/sessions for new invocations; LOGIN does not revive old tokens | Apply lifecycle atomically; role/member/token changes invalidate pending write authorization while bounded admitted reads retain snapshots |
| ID-6 | MANAGE_ROLES cannot change reserved database_admin membership; last usable admin removal fails | Current-actor commit revalidation, reserved-role/system-privilege checks and last-login-admin-with-credential protection |
| ID-7 | Another valid owner credential can reconcile outcomes, but revoked or cross-user credentials cannot | Own/admin status checks, no token/ID enumeration, concurrent revoke/commit tests and restart compatibility rejection for unknown verifier profiles |

## Acceptance and Handoff

Test authentication, administrative authority and write-commit authority separately through real channels. Audit lifecycle actions without secrets, exclude credentials from query parameters/artifacts/executor channels, and verify users gain no data access merely by being owners/admins. Automatic expiry may still cause lockout; do not misstate last-admin protection. First behavior: verified bootstrap token authentication and generic invalid-token rejection after the credential profile is specified.
