# Statistics Collection and Planning Estimates

## Decision and Rationale

The optimizer needs cardinality, selectivity, value-width, distinct-count, correlation, and access-path information to choose useful plans. It does not need unrestricted scans or direct access to database files to obtain that information. Separate collection from consumption: trusted storage code collects statistics, while the isolated planner requests authorized estimates.

Introduce a logical statistics service inside the storage manager for the baseline. It owns collection, a protected catalog, scoped estimation, and refresh scheduling. Moving collection into a separate trusted process is a future option, not a prerequisite. Such a collector would hold sensitive data and remain part of the confidentiality boundary.

## Two Statistics Domains

| Domain | Contents and use | Planner access |
| --- | --- | --- |
| Internal physical statistics | Global counts, pages, histograms, frequent values, samples, index properties, correlations | No direct access |
| Authorized-view estimates | Estimates derived from the execution's visible rows and readable cells, plus explicitly public schema/configuration | Bounded estimation API |

Global statistics are optional in the first prototype. They can later support storage maintenance and internal access-path decisions, subject to the architecture's timing-channel limitations. Do not return global counts, costs, catalog epochs, or plan recommendations indirectly through the scoped API unless their disclosure has been explicitly authorized.

## Collection

Use fixed, trusted Rust routines over storage snapshots; never load the C SDK into this trusted component. Do not invoke planner-supplied code, user-defined comparators, or native functions on private samples. Bound scan work, memory, catalog size, and refresh frequency. Schedule expensive maintenance separately from per-query planning; a planner cannot trigger an unrestricted `ANALYZE` operation.

Initially compute small authorized summaries on demand within a planning budget. If computation is too expensive, return `unknown` and let the optimizer use fixed heuristics. Later add scoped caches, incremental collection, or sampling. Sampling must account for row visibility and cell redaction; a global sample cannot simply be relabeled as representative of an authorized population.

## Planner Interface

Conceptual planning-channel operations:

```text
describe_relation(logical_relation_ref) -> planning_table_handle, authorized_schema_and_access_paths
estimate_scan(planning_table_handle, columns, predicate_ir) -> estimate
```

Planning table handles belong only to the optimizer connection and cannot be passed as executor scan handles. The connection supplies the context; requests cannot choose a principal or arbitrary policy version. Access-path descriptions expose approved logical capabilities, not physical page counts or private index contents. An estimate may contain:

- Estimated visible output rows and authorized output bytes.
- Estimated readable/null/redacted fractions for requested fields.
- Selectivity or distinct-count estimates with an explicit population definition.
- Method and uncertainty information, or `unknown`/`unsupported`.
- An opaque context-scoped validity token with no global catalog history.

Provide only fields justified by authorized data or public configuration. Histograms and raw samples are not part of the initial external API. Estimates must not conceal privileged data in errors, logs, response metadata, or returned physical plans.

### Initial On-Demand Summary

`InvocationTransaction::estimate_authorized_scan(table_id, projection)` returns either an exact authorized-view summary or `Unknown`. Exact summaries contain the visible-row count and, in requested-column order, VALUE/NULL/REDACTED cell counts. They contain no row values, widths, row IDs, partial counts, or predicate selectivity. Collection reads the invocation's pinned snapshot plus its private overlay and applies the same trusted row/cell projection as an authorized scan.

Each planner connection may request at most 16 estimates. The count is cumulative for the lifetime of its planner stream and survives metadata-session detach/reattach between requests. After the estimate allowance is exhausted, the storage interface returns `Unknown` without aborting the invocation, so the optimizer can continue with fixed heuristics. Each estimate is separately bounded to 16,384 candidate rows and returns `Unknown` without partial counts when that bound is reached or the request's context/schema cannot be validated. To keep the completion result independent of invisible-row volume, it only scans when it can prove row inclusion is value-independent: an applicable table SELECT policy must allow every row with a literal TRUE predicate, no applicable table policy may filter by row values, and at least one requested column must be granted. Value-dependent table policies return `Unknown` before scanning. Predicate selectivity and cached summaries remain unsupported. Cancellation and deadline expiry remain terminal rather than becoming `Unknown`.

## Cell-Specific Semantics

For a department manager, visible employee count may cover the department, but salary distributions may use only readable salaries of direct reports. State both populations explicitly. A readable-salary distribution must never stand in for the full department's salary distribution.

An internal summary of readable cells is advisory metadata, not permission to execute `SUM(salary)` while silently dropping redacted inputs. Estimation must preserve the query's redaction/error semantics. Predicates requiring redacted values cannot be evaluated against those values during estimation. Return an authorization outcome only if established by trusted validation; otherwise report an unknown estimate and leave definitive checks to execution. A sample showing no redactions is not proof of full authorization.

Multicolumn and future join estimates require authorization for all contributing inputs and explicit joint populations. Separate marginal summaries do not justify assumptions about hidden correlations.

## Disclosure and Trust

Assume the optimizer can encode anything it receives into a physical plan or diagnostic, which the user query library can then disclose through results. It does not generate native artifacts in the baseline. Consequently its statistics authority must not exceed the execution's data authority. Estimates derived from inaccessible cells are disclosures even if they are approximate, rounded, or returned only as costs. Repeated chosen predicates can turn such an API into a data-probing oracle.

The baseline allows summaries computable from the current authorized view. It does not claim full timing or cache noninterference. Rate limits bound work, not disclosure. A future privileged optimizer using global statistics would require an explicitly broader trust model and a reviewed disclosure policy; process separation by itself would not make it safe.

## Operational Priority

Keep collection and expensive estimates off the operational critical path when bounded heuristics suffice. Analytical estimates may use larger admitted budgets but cannot starve WAL/commit or expose global distributions. Evaluate plan quality and collection overhead with mixed operational/analytical workloads; a table-wide histogram is not a reason to change the row-oriented primary storage layout.

## Concurrent Collection

Estimate against the invocation's MVCC snapshot plus its successful private writes. Include a transaction-local write generation in estimate/cache validity and reject reuse after mutations. Never expose one invocation's private writes to another. Background collectors use their own registered committed snapshots, no private overlays, and publish catalog updates atomically.

Collectors and estimation tasks hold their own storage-managed snapshot/version pins. Release them on completion or drained cancellation so reclamation cannot race an in-flight scan. Apply independent work budgets and avoid holding catalog locks while scanning or sending replies. See [concurrency and MVCC](concurrency-mvcc.md).

## Catalog Integration

Logical object/type/schema identities come from the [data dictionary](data-dictionary.md). Protected statistics catalogs remain a separate derived store: entries reference source object versions, sampling method, and validity domain and are never ordinary metadata-discovery rows. Statistics storage/format may be discarded and rebuilt during [offline upgrades](catalog-lifecycle.md), but invalid estimates must not be reused. Live collection progress belongs to [operational metadata](operational-metadata.md), not optimizer-visible cardinalities or global counters.

## Freshness, Caches, and Failure

Bind summaries to the principal, effective grant, policy and policy-input snapshot, data snapshot, private-write generation, schema version, requested expression, and population. Reuse across contexts requires proof of equivalent visibility and validity; initially do not share scoped entries across principals.

Stale values can contain data no longer authorized under the current snapshot. Do not reuse historical statistics merely because inaccurate estimates are acceptable. Initial estimates use the pinned authorized snapshot; incompatible entries are recomputed or treated as unavailable.

Statistics never establish authorization or correctness. Missing or unsupported estimates, or exhaustion of an estimation-specific allowance, may fall back to fixed heuristics while the planning request remains active. Invocation/request cancellation, deadline expiry, or overall planning budget exhaustion terminates the affected work; fallback must not bypass cancellation or widen access. Keep validity of permissions separate from uncertainty in cardinality.

## Validation and Research Questions

- Compare scoped summaries with a reference authorized scan, including nulls and redactions.
- Change only hidden values or add invisible rows: normalized estimate content must remain unchanged when the authorized view and public inputs are unchanged. Test timing/resource behavior separately as a known limitation.
- Attempt repeated predicate probes, cross-principal cache reuse, stale-policy reuse, and statistics access through worker channels.
- Test planning work limits, cancellation, unsupported expressions, and heuristic fallback.
- Measure planning overhead and plan quality under highly selective policies before adding richer statistics.

Open choices include collection algorithms, safe sampling, cache equivalence proofs, policy-aware cardinality estimation, and whether an explicitly privileged planning mode is ever desirable.

## Background

PostgreSQL separates statistics collection through [`ANALYZE`](https://www.postgresql.org/docs/18/sql-analyze.html) from planner consumption. Its [planner statistics security documentation](https://www.postgresql.org/docs/18/planner-stats-security.html) explains how stored values can leak through user-defined operators. PostgreSQL permits some generic statistics independently of access privileges; this design deliberately adopts a stricter disclosure boundary for true cell-specific permissions.
