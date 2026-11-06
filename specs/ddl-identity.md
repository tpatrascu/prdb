# Database Users, Roles, and Authentication

## Identity Model

A database user is a named login principal with a stable opaque ID. A role is a named permission group, not a login. Both are catalog objects independent of OS user IDs. The supervisor authenticates a caller and storage binds that user's ID, role closure, identity epoch, and requested scope to an invocation. Query parameters and native code cannot choose another principal.

Initially support database-local users and roles with database-issued opaque bearer tokens. Passwords, federated identity, OS-credential mapping, and impersonation are separate future authentication mechanisms. Client query/control connections use the [unencrypted TCP gateway](client-gateway.md), loopback-only by default. Token validation establishes database identity; TCP itself supplies neither encryption nor server authentication. Non-loopback plaintext exposure is explicit operator configuration for a trusted network, not a secure remote profile. Bootstrap/offline setup and internal service channels retain permission-restricted Unix sockets. Never treat the local machine's shared UID as sufficient to identify every database caller.

## User and Role DDL

```sql
CREATE USER alice LOGIN;
CREATE USER service_loader NOLOGIN;
ALTER USER alice NOLOGIN;
ALTER USER alice LOGIN;
ALTER USER alice RENAME TO alice_admin;
DROP USER service_loader RESTRICT;

CREATE ROLE managers;
CREATE ROLE app_owner;
GRANT managers TO USER alice_admin;
GRANT managers TO ROLE app_owner;
REVOKE managers FROM USER alice_admin;
DROP ROLE managers RESTRICT;
```

Users and roles share one unambiguous naming namespace. Creation denies data privileges by default. Rename retains the stable ID and all references. Membership inheritance is transitive; cycles and self-membership fail. `GRANT role TO ROLE parent` makes members of `parent` inherit `role`. Every membership edge must name a live user or role as its member and a live role as its target. The runtime resolves a pinned closure from at most 4096 membership edges and fails closed on cycles, duplicate edges, or work-limit overflow. Administrative membership commits validate endpoint kinds and the complete final graph, including disconnected components, against the latest committed graph plus staged changes under commit serialization; the 4096-edge limit is enforced before publication. No `SET ROLE`, delegation/admin option, or `PUBLIC` role in the baseline. Policies targeting a role apply to its transitive members.

`NOLOGIN` blocks new authentication/admission and revokes all the user's tokens and authenticated sessions for new invocations; returning to `LOGIN` requires issuing a fresh token. Users with owned objects, grants, membership, or policy references cannot be dropped under `RESTRICT`; transfer/revoke dependencies explicitly. Credentials owned by an otherwise droppable user are revoked as part of deletion. Dropping an identity never permits ID reuse. Role removal changes membership epochs and access for new invocations.

## Credential Lifecycle

Credential operations are authenticated control API calls, not literal-bearing DDL:

```text
issue_token(user_id, label, expires_at) -> token_id, secret_once
revoke_token(token_id)
list_tokens(user_id) -> nonsecret_metadata
```

Issuance requires `MANAGE_USERS` and a login-enabled target. Generate high-entropy random secrets in trusted code, return once through the protected control connection, and store only a cryptographic verifier. Tokens have expiry, identity binding, revocation state, and an ID distinct from their secret. Do not persist plaintext secrets in catalog exports, SQL text, query parameters, artifacts, audit records, or logs. Clients send credentials only to admission, never to executors. Validate expiry/revocation on every new invocation, including connections already authenticated.

Rotation issues a replacement and revokes the old token; revocation does not imply another token for the same user is revoked. Queries cannot issue credentials. Administrative audit records contain actor ID, target ID, action, and outcome, never the secret. Algorithm, wire format, rate limits, and secure client-side credential storage must be specified before implementation.

## Bootstrap and Administration

Initialization creates one bootstrap user and a usable credential through a local, permission-restricted setup flow; there is no default password or anonymous administrator. The user belongs to the reserved `database_admin` role, which cannot be dropped. Only existing database administrators may change that role's membership, change membership in a way that alters its inherited membership closure, or grant/revoke system privileges; MANAGE_ROLES alone is insufficient. Ordinary role membership changes require `MANAGE_ROLES` or database-administrator membership. Storage rejects membership changes that leave no login-enabled database administrator; credential usability must also be checked once supported verifier records are available. The system-privilege DDL route handles only database-scoped `MANAGE_USERS`, `MANAGE_ROLES`, and `CREATE_SCHEMA` grants/revokes. Administrators have catalog-control privileges, not a hidden exemption for executor data access. They can deliberately change grants/policies, so they are trusted to control confidentiality.

Administrative system privileges are `MANAGE_USERS`, `MANAGE_ROLES`, and `CREATE_SCHEMA`; schema/table authority is defined in [permission DDL](ddl-permissions.md). Bootstrap owns these privileges and may assign them. `MANAGE_USERS` can create/drop/disable users and manage credentials; `MANAGE_ROLES` can create/drop roles and change membership. This is powerful administrative authority, not an SDK query role.

Administrative batches revalidate the actor's current authority before commit; a stale administrative snapshot cannot resurrect revoked privileges. Reject administrative changes or explicit token revocations that would leave no login-enabled database administrator with a usable credential in the baseline. Automatic credential expiry can still cause lockout; setup/rotation must provide a deliberate recovery credential policy. Disaster recovery/reset procedures remain a separate design task.

## Sessions, Transactions, and Revocation

Database user/role IDs are unrelated to IPC service roles such as executor or optimizer. Authentication yields a user; the supervisor provisions service channels. Effective data authority is the intersection of current admitted user permissions and the requested invocation grant.

After a trusted caller verifies a credential, `StorageManager::begin_authenticated_invocation` checks that the supplied identity is a live `LOGIN` user in the invocation's pinned catalog snapshot and returns its pinned role closure. It guards the identity and membership records used for admission. Credential verification, token expiry, and revocation checks must happen before this call; the method does not authenticate a caller-supplied ID by itself.

Pin role membership and identity/policy epochs for the transaction. Token revocation, user disable/drop, or membership changes block new invocations and make affected pending writes fail commit validation. Already admitted reads retain their bounded snapshot; immediate read revocation is not promised. An explicit cancel request may terminate active invocations, but cannot recall delivered bytes or undo committed writes.

Outcome lookup requires the authenticated originating user or an authorized administrator. No cross-user enumeration of invocation IDs, token inventories, or role membership. An expired/revoked credential cannot be used for status lookup; the caller must authenticate with another valid credential or seek an administrator.

## Catalog and Operational Integration

Identity, membership, grant, and verifier records follow the [data dictionary](data-dictionary.md). Bootstrap uses the recoverable INITIALIZING/READY protocol in [catalog lifecycle](catalog-lifecycle.md), including lost secret-handoff handling; ordinary startup never recreates an administrator. Unknown verifier algorithms fail compatibility checks rather than weakening authentication.

Administrative lifecycle actions produce atomic audit/outbox records under [operational metadata](operational-metadata.md). Invocation status uses the durable outcome store after restart; missing/expired history cannot be reported as a definite abort. Verifier bytes remain internal even for user administrators.

## Validation

Test spoofed IDs, login-disabled users, token expiry and rotation, membership cycles, role inheritance, rename stability, restricted drops, last-admin protection, revoked sessions starting new invocations, identity changes racing commit, and absence of secrets in logs/artifacts. Authentication, authority, and commit validation must be tested separately.
